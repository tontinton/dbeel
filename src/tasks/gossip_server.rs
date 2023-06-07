use std::{any::Any, rc::Rc};

use glommio::{net::UdpSocket, spawn_local, Task};
use log::{error, trace};

use crate::{
    error::Result, gossip::deserialize_gossip_message, shards::MyShard,
};

const UDP_PACKET_BUFFER_SIZE: usize = 65536;

async fn handle_gossip_packet(
    my_shard: Rc<MyShard>,
    packet_buf: &[u8],
) -> Result<()> {
    let message = deserialize_gossip_message(packet_buf)?;

    // Check whether we have seen this gossip event enough times.
    let seen_first_time = {
        let mut requests = my_shard.gossip_requests.borrow_mut();
        let seen_count = requests
            .entry((message.source, message.event.type_id()))
            .or_insert(0);

        // TODO: delete entries.
        if *seen_count >= my_shard.args.gossip_max_seen_count {
            return Ok(());
        }

        *seen_count += 1;
        *seen_count == 1
    };

    let continue_with_gossip = if seen_first_time {
        trace!("Gossip: {:?}", message.event);
        my_shard.clone().handle_gossip_event(message.event).await?
    } else {
        true
    };

    if continue_with_gossip {
        my_shard.gossip_buffer(packet_buf).await?;
    }

    Ok(())
}

async fn run_gossip_server(my_shard: Rc<MyShard>) -> Result<()> {
    let address = format!("{}:{}", my_shard.args.ip, my_shard.args.gossip_port);
    let server = UdpSocket::bind(address.as_str())?;
    trace!("Listening for gossip packets on: {}", address);

    let mut buf = vec![0; UDP_PACKET_BUFFER_SIZE];

    loop {
        match server.recv_from(&mut buf).await {
            Ok((n, _client_address)) => {
                if let Err(e) =
                    handle_gossip_packet(my_shard.clone(), &buf[..n]).await
                {
                    error!("Failed to handle gossip packet: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to recv gossip packet: {}", e);
            }
        }
    }
}

pub fn spawn_gossip_server_task(my_shard: Rc<MyShard>) -> Task<Result<()>> {
    spawn_local(async move {
        let result = run_gossip_server(my_shard).await;
        if let Err(e) = &result {
            error!("Error starting gossip server: {}", e);
        }
        result
    })
}
