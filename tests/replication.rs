use std::{sync::Once, time::Duration};

use dbeel::{
    args::{parse_args_from, Args},
    error::Result,
    flow_events::FlowEvent,
};
use dbeel_client::{Consistency, DbeelClient};
use futures::try_join;
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
    parse_args_from(["", "--dir", "/tmp/test"])
}

fn three_nodes_replication_test(
    args: Args,
    set_consistency: usize,
    get_consistency: usize,
) -> Result<()> {
    let (seed_sender, seed_receiver) = async_channel::bounded(1);
    let (set1_sender, set1_receiver) = async_channel::bounded(1);
    let (set2_sender, set2_receiver) = async_channel::bounded(1);
    let (collection_created1_sender, collection_created1_receiver) =
        async_channel::bounded(1);
    let (collection_created2_sender, collection_created2_receiver) =
        async_channel::bounded(1);
    let (done_sender, done_receiver) = async_channel::bounded(1);
    let (done1_sender, done1_receiver) = async_channel::bounded(1);
    let (done2_sender, done2_receiver) = async_channel::bounded(1);

    let main_handle = test_node(1, args.clone(), move |shard, _| async move {
        seed_sender
            .send(vec![format!(
                "{}:{}",
                shard.args.ip,
                shard.args.remote_shard_port + shard.id
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

        let collection_created =
            shard.subscribe_to_flow_event(FlowEvent::CollectionCreated.into());
        let collection = client
            .create_collection_with_replication("test", 3)
            .await
            .unwrap();

        try_join!(
            collection_created.recv(),
            collection_created1_receiver.recv(),
            collection_created2_receiver.recv(),
        )
        .unwrap();

        collection
            .set_consistent(
                Value::String("key".into()),
                Value::F32(42.0),
                Consistency::Fixed(set_consistency),
            )
            .await
            .unwrap();
        try_join!(set1_sender.send(()), set2_sender.send(())).unwrap();

        // Wait for all other nodes to finish their tests.
        done_receiver.recv().await.unwrap();
    })?;

    let seed_nodes = seed_receiver.recv_blocking()?;

    let mut args1 = next_node_args(args, "first".to_string(), 1);
    args1.dir = "/tmp/test1".to_string();
    args1.seed_nodes = seed_nodes;
    let mut args2 = next_node_args(args1.clone(), "second".to_string(), 1);
    args2.dir = "/tmp/test2".to_string();

    let mut handles = Vec::new();

    for (
        node_args,
        set_receiver,
        collection_created_sender,
        done_sender,
        done_receiver,
    ) in vec![
        (
            args1,
            set1_receiver,
            collection_created1_sender,
            done1_sender,
            done2_receiver,
        ),
        (
            args2,
            set2_receiver,
            collection_created2_sender,
            done2_sender,
            done1_receiver,
        ),
    ] {
        handles.push(test_node(1, node_args, move |shard, _| async move {
            if shard.collections.borrow().is_empty() {
                let receiver = shard.subscribe_to_flow_event(
                    FlowEvent::CollectionCreated.into(),
                );
                receiver.recv().await.unwrap();
            }
            collection_created_sender.send(()).await.unwrap();
            set_receiver.recv().await.unwrap();

            let mut client = DbeelClient::from_seed_nodes(&[(
                shard.args.ip.clone(),
                shard.args.port,
            )])
            .await
            .unwrap();

            client.set_read_timeout(Duration::from_secs(1));
            client.set_write_timeout(Duration::from_secs(1));

            let collection = client.collection("test").await.unwrap();
            let response = collection
                .get_consistent(
                    Value::String("key".into()),
                    Consistency::Fixed(get_consistency),
                )
                .await
                .unwrap();
            let value = read_value_ref(&mut &response[..]).unwrap();
            assert_eq!(value, ValueRef::F32(42.0));

            done_sender.send(()).await.unwrap();
            done_receiver.recv().await.unwrap();
        })?);
    }

    for handle in handles {
        handle.join()?;
    }
    done_sender.send_blocking(())?;
    main_handle.join()?;

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
