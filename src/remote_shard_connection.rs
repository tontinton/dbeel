use std::time::Duration;

use bincode::{
    config::{
        FixintEncoding, RejectTrailing, WithOtherIntEncoding, WithOtherTrailing,
    },
    DefaultOptions, Options,
};
use futures_lite::{AsyncRead, AsyncWrite, AsyncWriteExt};
use glommio::net::TcpStream;

use crate::{
    error::{Error, Result},
    messages::{NodeMetadata, ShardMessage, ShardRequest, ShardResponse},
    read_exactly::read_exactly,
};

#[derive(Debug, Clone)]
pub struct RemoteShardConnection {
    // The address to use to connect to the remote shard.
    pub address: String,
    connect_timeout: Duration,
}

impl RemoteShardConnection {
    pub fn new(address: String, connect_timeout: Duration) -> Self {
        Self {
            address,
            connect_timeout,
        }
    }

    async fn connect(&self) -> Result<TcpStream> {
        Ok(
            TcpStream::connect_timeout(&self.address, self.connect_timeout)
                .await?,
        )
    }

    pub async fn send_request(
        &self,
        request: ShardRequest,
    ) -> Result<ShardResponse> {
        let mut stream = self.connect().await?;
        send_message_to_stream(&mut stream, &ShardMessage::Request(request))
            .await?;
        let response = match get_message_from_stream(&mut stream).await? {
            ShardMessage::Response(response) => response,
            _ => return Err(Error::ResponseWrongType),
        };
        stream.close().await?;
        Ok(response)
    }

    pub async fn ping(&self) -> Result<()> {
        match self.send_request(ShardRequest::Ping).await? {
            ShardResponse::Pong => Ok(()),
            _ => Err(Error::ResponseWrongType),
        }
    }

    pub async fn get_metadata(&self) -> Result<Vec<NodeMetadata>> {
        match self.send_request(ShardRequest::GetMetadata).await? {
            ShardResponse::GetMetadata(metadata) => Ok(metadata),
            _ => Err(Error::ResponseWrongType),
        }
    }
}

fn bincode_options() -> WithOtherIntEncoding<
    WithOtherTrailing<DefaultOptions, RejectTrailing>,
    FixintEncoding,
> {
    DefaultOptions::new()
        .reject_trailing_bytes()
        .with_fixint_encoding()
}

pub async fn get_message_from_stream(
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<ShardMessage> {
    let size_buf = read_exactly(stream, 4).await?;
    let size = u32::from_le_bytes(size_buf.as_slice().try_into().unwrap());
    let request_buf = read_exactly(stream, size as usize).await?;

    let mut cursor = std::io::Cursor::new(&request_buf[..]);

    Ok(bincode_options().deserialize_from::<_, ShardMessage>(&mut cursor)?)
}

pub async fn send_message_to_stream(
    stream: &mut (impl AsyncWrite + Unpin),
    message: &ShardMessage,
) -> Result<()> {
    let msg_buf = bincode_options().serialize(message)?;
    let size_buf = (msg_buf.len() as u32).to_le_bytes();

    stream.write_all(&size_buf).await?;
    stream.write_all(&msg_buf).await?;

    Ok(())
}
