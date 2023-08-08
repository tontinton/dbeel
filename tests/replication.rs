use futures::try_join;
use std::{sync::Once, time::Duration};

use dbeel::{
    args::{parse_args_from, Args},
    error::Result,
    flow_events::FlowEvent,
};
use dbeel_client::DbeelClient;
use rmpv::{decode::read_value_ref, Value, ValueRef};
use rstest::{fixture, rstest};
use serial_test::serial;
use test_utils::{install_logger, next_node_args, test_node};

static ONCE: Once = Once::new();

#[fixture]
fn args() -> Args {
    ONCE.call_once(|| {
        install_logger();
    });

    // Remove the test directories if it exists.
    let _ = std::fs::remove_dir_all("/tmp/test");
    let _ = std::fs::remove_dir_all("/tmp/test1");
    let _ = std::fs::remove_dir_all("/tmp/test2");
    parse_args_from(["", "--dir", "/tmp/test", "--replication-factor", "3"])
}

fn three_nodes_replication_test(
    args: Args,
    set_consistency: usize,
    get_consistency: usize,
) -> Result<()> {
    let (seed_sender, seed_receiver) = async_channel::bounded(1);
    let (key_set_sender1, key_set_receiver1) = async_channel::bounded(1);
    let (key_set_sender2, key_set_receiver2) = async_channel::bounded(1);

    let mut handles = Vec::new();

    handles.push(test_node(1, args.clone(), move |shard, _| async move {
        seed_sender
            .send(vec![format!(
                "{}:{}",
                shard.args.ip,
                shard.args.remote_shard_port + shard.id as u16
            )])
            .await
            .unwrap();
        while shard.nodes.borrow().len() < 2 {
            let receiver = shard
                .subscribe_to_flow_event(FlowEvent::AliveNodeGossip.into());
            receiver.recv().await.unwrap();
        }
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();
        let collection = client.create_collection("test").await.unwrap();
        collection
            .set_consistent("key", Value::F32(42.0), set_consistency)
            .await
            .unwrap();
        try_join!(key_set_sender1.send(()), key_set_sender2.send(())).unwrap();
    })?);

    let seed_nodes = seed_receiver.recv_blocking()?;

    let mut args1 = next_node_args(args, "first".to_string(), 1);
    args1.dir = "/tmp/test1".to_string();
    args1.seed_nodes = seed_nodes;
    let mut args2 = next_node_args(args1.clone(), "second".to_string(), 1);
    args2.dir = "/tmp/test2".to_string();

    for (key_set_receiver, node_args) in
        vec![(key_set_receiver1, args1), (key_set_receiver2, args2)]
    {
        handles.push(test_node(1, node_args, move |shard, _| async move {
            key_set_receiver.recv().await.unwrap();
            let mut client = DbeelClient::from_seed_nodes(&[(
                shard.args.ip.clone(),
                shard.args.port,
            )])
            .await
            .unwrap();

            client.set_read_timeout(Duration::from_secs(1));
            client.set_write_timeout(Duration::from_secs(1));

            let collection = client.collection("test");
            let response = collection
                .get_consistent("key", get_consistency)
                .await
                .unwrap();
            let value = read_value_ref(&mut &response[..]).unwrap();
            assert_eq!(value, ValueRef::F32(42.0));
        })?);
    }

    for handle in handles {
        handle.join()?;
    }

    Ok(())
}

#[rstest]
#[serial]
fn set_replication(args: Args) -> Result<()> {
    three_nodes_replication_test(args, 3, 1)
}

#[rstest]
#[serial]
fn get_replication(args: Args) -> Result<()> {
    three_nodes_replication_test(args, 1, 3)
}
