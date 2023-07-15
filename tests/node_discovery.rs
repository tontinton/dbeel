use std::sync::Once;

use dbeel::{
    args::{parse_args_from, Args},
    error::Result,
    flow_events::FlowEvent,
    messages::NodeMetadata,
};
use rstest::{fixture, rstest};
use test_utils::{install_logger, test_node, test_shard};

static ONCE: Once = Once::new();

fn create_metadata_from_args(
    args: Args,
    number_of_shards: u16,
) -> NodeMetadata {
    NodeMetadata {
        name: args.name,
        ip: args.ip,
        shard_ports: (0..number_of_shards)
            .into_iter()
            .map(|x| x + args.remote_shard_port)
            .collect::<Vec<_>>(),
        gossip_port: args.gossip_port,
    }
}

#[fixture]
fn args() -> Args {
    ONCE.call_once(|| {
        install_logger();
    });

    // Remove the test directory if it exists.
    let _ = std::fs::remove_dir("/tmp/test");
    parse_args_from(["", "--dir", "/tmp/test"])
}

#[rstest]
fn clean_state(args: Args) -> Result<()> {
    test_shard(args, |shard| async move {
        assert!(shard.nodes.borrow().is_empty());
    })
}

#[rstest]
fn find_nodes(args: Args) -> Result<()> {
    let number_of_shards_first_node = 2u16;
    let mut second_args = args.clone();
    let (seed_sender, seed_receiver) = async_channel::bounded(1);
    let (second_up_sender, second_up_receiver) = async_channel::bounded(1);

    let first_handle = test_node(
        number_of_shards_first_node.into(),
        args.clone(),
        move |node_shard, _| async move {
            let second_dead_event = node_shard
                .subscribe_to_flow_event(FlowEvent::DeadNodeRemoved.into());

            seed_sender
                .send(vec![format!(
                    "{}:{}",
                    node_shard.args.ip,
                    node_shard.args.remote_shard_port + node_shard.id as u16
                )])
                .await
                .unwrap();
            let second_args: Args = second_up_receiver.recv().await.unwrap();
            assert_eq!(node_shard.nodes.borrow().len(), 1);
            assert_eq!(
                node_shard.nodes.borrow().get(&second_args.name).unwrap(),
                &create_metadata_from_args(second_args, 1)
            );
            second_dead_event.recv().await.unwrap();
            assert!(node_shard.nodes.borrow().is_empty());
        },
    )?;

    let seed_nodes = seed_receiver.recv_blocking()?;
    second_args.seed_nodes = seed_nodes;
    second_args.remote_shard_port += number_of_shards_first_node;
    second_args.port += number_of_shards_first_node;
    second_args.gossip_port += number_of_shards_first_node;
    second_args.name = "second".to_string();

    let second_handle =
        test_node(1, second_args.clone(), move |node_shard, _| async move {
            second_up_sender.send(second_args).await.unwrap();
            assert_eq!(node_shard.nodes.borrow().len(), 1);
            assert_eq!(
                node_shard.nodes.borrow().get(&args.name).unwrap(),
                &create_metadata_from_args(args, number_of_shards_first_node)
            );
        })?;

    second_handle.join()?;
    first_handle.join()?;

    Ok(())
}
