use std::rc::Rc;

use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use glommio::{enclose, net::TcpListener, spawn_local, Task};
use log::{error, trace};

use crate::{
    error::Result,
    messages::ShardMessage,
    remote_shard_connection::{
        get_message_from_stream, send_message_to_stream,
    },
    shards::MyShard,
};

async fn handle_remote_shard_client(
    my_shard: Rc<MyShard>,
    client: &mut (impl AsyncRead + AsyncWrite + Unpin),
) -> Result<()> {
    let msg = get_message_from_stream(client).await?;
    if let Some(response_msg) = my_shard.handle_shard_message(msg).await? {
        send_message_to_stream(client, &ShardMessage::Response(response_msg))
            .await?;
    }
    client.close().await?;

    Ok(())
}

async fn run_remote_shard_server(my_shard: Rc<MyShard>) -> Result<()> {
    let address = format!(
        "{}:{}",
        my_shard.args.ip,
        my_shard.args.remote_shard_port + my_shard.id as u16
    );
    let server = TcpListener::bind(address.as_str())?;
    trace!("Listening for distributed messages on: {}", address);

    loop {
        match server.accept().await {
            Ok(mut client) => {
                spawn_local(enclose!((my_shard.clone() => my_shard) async move {
                    let result =
                        handle_remote_shard_client(my_shard, &mut client).await;
                    if let Err(e) = result {
                        error!("Failed to handle distributed client: {}", e);
                    }
                }))
                .detach();
            }
            Err(e) => {
                error!("Failed to accept client: {}", e);
            }
        }
    }
}

pub fn spawn_remote_shard_server_task(
    my_shard: Rc<MyShard>,
) -> Task<Result<()>> {
    spawn_local(async move {
        let result = run_remote_shard_server(my_shard).await;
        if let Err(e) = &result {
            error!("Error starting remote shard server: {}", e);
        }
        result
    })
}
