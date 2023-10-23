use std::rc::Rc;

use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use glommio::{
    enclose, executor, net::TcpListener, spawn_local, spawn_local_into,
    Latency, Shares, Task,
};
use log::{error, trace};

use crate::{
    error::{Error, Result},
    messages::{ShardMessage, ShardResponse},
    remote_shard_connection::{
        get_message_from_stream, send_message_to_stream,
    },
    shards::MyShard,
};

async fn handle_remote_shard_client(
    my_shard: Rc<MyShard>,
    client: &mut (impl AsyncRead + AsyncWrite + Unpin),
) -> Result<()> {
    loop {
        match get_message_from_stream(client).await {
            Ok(msg) => match my_shard.clone().handle_shard_message(msg).await {
                Ok(Some(response_msg)) => {
                    send_message_to_stream(
                        client,
                        &ShardMessage::Response(response_msg),
                    )
                    .await?;
                }
                Ok(None) => {}
                Err(e) => {
                    send_message_to_stream(
                        client,
                        &ShardMessage::Response(ShardResponse::Error(
                            e.to_string(),
                        )),
                    )
                    .await?;
                }
            },
            Err(Error::StdIOError(e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break
            }
            Err(e) => return Err(e),
        }
    }
    client.close().await?;

    Ok(())
}

async fn run_remote_shard_server(my_shard: Rc<MyShard>) -> Result<()> {
    let address = format!(
        "{}:{}",
        my_shard.args.ip,
        my_shard.args.remote_shard_port + my_shard.id
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
                        error!("Failed to handle remote shard client: {}", e);
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
    let shares = my_shard.args.background_tasks_shares.into();
    spawn_local_into(
        async move {
            let result = run_remote_shard_server(my_shard).await;
            if let Err(e) = &result {
                error!("Error starting remote shard server: {}", e);
            }
            result
        },
        executor().create_task_queue(
            Shares::Static(shares),
            Latency::NotImportant,
            "remote-shard-server",
        ),
    )
    .unwrap()
}
