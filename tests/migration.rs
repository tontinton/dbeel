use std::{sync::Once, time::Duration};

use dbeel::{
    args::{parse_args_from, Args},
    error::Result,
    flow_events::FlowEvent,
};
use dbeel_client::DbeelClient;
use futures::try_join;
use once_cell::sync::Lazy;
use rmpv::{encode::write_value, Value};
use rstest::{fixture, rstest};
use serial_test::serial;
use test_utils::{install_logger, next_node_args, test_node};

static ONCE: Once = Once::new();

static UPPER_KEY: Lazy<Vec<u8>> = Lazy::new(|| {
    let mut v = Vec::new();
    write_value(&mut v, &Value::String("KEY".into())).unwrap();
    v
});
static UPPER_VALUE: Lazy<Vec<u8>> = Lazy::new(|| {
    let mut v = Vec::new();
    write_value(&mut v, &Value::Boolean(false)).unwrap();
    v
});
static LOWER_KEY: Lazy<Vec<u8>> = Lazy::new(|| {
    let mut v = Vec::new();
    write_value(&mut v, &Value::String("key".into())).unwrap();
    v
});
static LOWER_VALUE: Lazy<Vec<u8>> = Lazy::new(|| {
    let mut v = Vec::new();
    write_value(&mut v, &Value::F32(42.0)).unwrap();
    v
});

#[fixture]
fn args() -> Args {
    ONCE.call_once(|| {
        install_logger();
    });

    // Remove the test directories if it exists.
    let _ = std::fs::remove_dir_all("/tmp/test");
    let _ = std::fs::remove_dir_all("/tmp/test1");
    let _ = std::fs::remove_dir_all("/tmp/test2");
    parse_args_from(["", "--dir", "/tmp/test", "--replication-factor", "2"])
}

#[rstest]
#[serial]
fn migration_on_death(args: Args) -> Result<()> {
    // "a-0"    -> 2727548292
    // "b-0"    -> 1121949192
    // "c-0"    -> 2242724227
    // "key"    -> 1211368233
    // "KEY"    -> 791967430
    //
    // KEY -> b-0 -> key -> c-0 -> a-0.

    let (seed_sender, seed_receiver) = async_channel::bounded(1);

    let (a_set_sender, a_set_receiver) = async_channel::bounded(1);
    let (b_set_sender, b_set_receiver) = async_channel::bounded(1);

    let (a_checked_sender, a_checked_receiver) = async_channel::bounded(1);
    let (b_checked_sender, b_checked_receiver) = async_channel::bounded(1);

    let (a_done_sender, a_done_receiver) = async_channel::bounded(1);
    let (b_done_sender, b_done_receiver) = async_channel::bounded(1);

    let mut c_args = args;
    c_args.name = "c".to_string();

    let c_handle = test_node(1, c_args.clone(), move |shard, _| async move {
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

        let mut client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        client.set_read_timeout(Duration::from_secs(1));
        client.set_write_timeout(Duration::from_secs(1));

        let collection_created =
            shard.subscribe_to_flow_event(FlowEvent::CollectionCreated.into());
        let collection = client.create_collection("test").await.unwrap();
        collection_created.recv().await.unwrap();

        collection
            .set_consistent(Value::String("key".into()), Value::F32(42.0), 2)
            .await
            .unwrap();
        collection
            .set_consistent(
                Value::String("KEY".into()),
                Value::Boolean(false),
                2,
            )
            .await
            .unwrap();

        assert_eq!(
            shard.trees.borrow()["test"].get(&UPPER_KEY).await.unwrap(),
            Some((*UPPER_VALUE).clone())
        );
        assert_eq!(
            shard.trees.borrow()["test"].get(&LOWER_KEY).await.unwrap(),
            Some((*LOWER_VALUE).clone())
        );

        try_join!(a_set_sender.send(()), b_set_sender.send(())).unwrap();
        try_join!(a_checked_receiver.recv(), b_checked_receiver.recv())
            .unwrap();
    })?;

    let seed_nodes = seed_receiver.recv_blocking()?;

    let mut a_args = next_node_args(c_args, "a".to_string(), 1);
    a_args.dir = "/tmp/test1".to_string();
    a_args.seed_nodes = seed_nodes;

    let mut b_args = next_node_args(a_args.clone(), "b".to_string(), 1);
    b_args.dir = "/tmp/test2".to_string();

    let mut handles = Vec::with_capacity(2);
    for (
        args,
        set_receiver,
        checked_sender,
        done_sender,
        done_receiver,
        should_own_upper,
    ) in vec![
        (
            a_args,
            a_set_receiver,
            a_checked_sender,
            a_done_sender,
            b_done_receiver,
            false,
        ),
        (
            b_args,
            b_set_receiver,
            b_checked_sender,
            b_done_sender,
            a_done_receiver,
            true,
        ),
    ] {
        let (up_sender, up_receiver) = async_channel::bounded(1);

        handles.push(test_node(1, args, move |shard, _| async move {
            up_sender.send(()).await.unwrap();
            set_receiver.recv().await.unwrap();

            assert_eq!(
                shard.trees.borrow()["test"].get(&UPPER_KEY).await.unwrap(),
                if should_own_upper {
                    Some((*UPPER_VALUE).clone())
                } else {
                    None
                },
            );
            assert_eq!(
                shard.trees.borrow()["test"].get(&LOWER_KEY).await.unwrap(),
                if !should_own_upper {
                    Some((*LOWER_VALUE).clone())
                } else {
                    None
                },
            );

            let item_migrated = shard.subscribe_to_flow_event(
                FlowEvent::ItemSetFromShardMessage.into(),
            );
            checked_sender.send(()).await.unwrap();
            item_migrated.recv().await.unwrap();

            assert_eq!(
                shard.trees.borrow()["test"].get(&UPPER_KEY).await.unwrap(),
                Some((*UPPER_VALUE).clone())
            );
            assert_eq!(
                shard.trees.borrow()["test"].get(&LOWER_KEY).await.unwrap(),
                Some((*LOWER_VALUE).clone())
            );

            done_sender.send(()).await.unwrap();
            done_receiver.recv().await.unwrap();
        })?);

        up_receiver.recv_blocking().unwrap();
    }

    c_handle.join()?;
    for handle in handles {
        handle.join()?;
    }

    Ok(())
}
