use std::time::Duration;

use bincode::Options;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use glommio::net::TcpStream;

use crate::{
    args::Args,
    error::{Error, Result},
    messages::{NodeMetadata, ShardMessage, ShardRequest, ShardResponse},
    response_to_empty_result, response_to_result,
    utils::bincode::bincode_options,
};

#[derive(Debug, Clone)]
pub struct RemoteShardConnection {
    // The address to use to connect to the remote shard.
    pub address: String,
    connect_timeout: Duration,
    write_timeout: Duration,
    read_timeout: Duration,
}

impl RemoteShardConnection {
    pub fn from_args(address: String, args: &Args) -> Self {
        Self::new(
            address,
            Duration::from_millis(args.remote_shard_connect_timeout),
            Duration::from_millis(args.remote_shard_write_timeout),
            Duration::from_millis(args.remote_shard_read_timeout),
        )
    }

    pub fn new(
        address: String,
        connect_timeout: Duration,
        write_timeout: Duration,
        read_timeout: Duration,
    ) -> Self {
        Self {
            address,
            connect_timeout,
            write_timeout,
            read_timeout,
        }
    }

    pub async fn connect(&self) -> Result<TcpStream> {
        let stream =
            TcpStream::connect_timeout(&self.address, self.connect_timeout)
                .await?;
        stream.set_write_timeout(Some(self.write_timeout))?;
        stream.set_read_timeout(Some(self.read_timeout))?;
        Ok(stream)
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
        response_to_empty_result!(
            self.send_request(ShardRequest::Ping).await?,
            ShardResponse::Pong
        )
    }

    pub async fn get_metadata(&self) -> Result<Vec<NodeMetadata>> {
        response_to_result!(
            self.send_request(ShardRequest::GetMetadata).await?,
            ShardResponse::GetMetadata
        )
    }

    pub async fn get_collections(&self) -> Result<Vec<(String, u16)>> {
        response_to_result!(
            self.send_request(ShardRequest::GetCollections).await?,
            ShardResponse::GetCollections
        )
    }
}

pub async fn get_message_from_stream(
    stream: &mut (impl AsyncRead + Unpin),
) -> Result<ShardMessage> {
    let mut size_buf = [0; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = u32::from_le_bytes(size_buf);
    let mut request_buf = vec![0; size as usize];
    stream.read_exact(&mut request_buf).await?;

    Ok(bincode_options()
        .deserialize_from::<_, ShardMessage>(&mut &request_buf[..])?)
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
