use std::sync::Once;

use dbeel::{
    args::{parse_args_from, Args},
    error::Result,
    flow_events::FlowEvent,
    messages::NodeMetadata,
};
use rstest::{fixture, rstest};
use test_utils::{
    install_logger, subscribe_to_flow_events, test_node, test_shard,
    wait_for_flow_events,
};

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
    let _ = std::fs::remove_dir_all("/tmp/test");
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
    let number_of_shards_second_node = 2u16;

    let mut second_args = args.clone();
    let (seed_sender, seed_receiver) = async_channel::bounded(1);
    let (second_up_sender, second_up_receiver) = async_channel::bounded(1);
    let (first_test_done_sender, first_test_done_receiver) =
        async_channel::bounded(1);

    let first_handle = test_node(
        number_of_shards_first_node.into(),
        args.clone(),
        move |node_shard, other_shards| async move {
            let mut all_shards = other_shards.clone();
            all_shards.push(node_shard.clone());

            let other_shards_alive_node_gossip_events =
                subscribe_to_flow_events(
                    &other_shards,
                    FlowEvent::AliveNodeGossip,
                );
            let all_second_node_dead_events = subscribe_to_flow_events(
                &all_shards,
                FlowEvent::DeadNodeRemoved,
            );

            seed_sender
                .send(vec![format!(
                    "{}:{}",
                    node_shard.args.ip,
                    node_shard.args.remote_shard_port + node_shard.id as u16
                )])
                .await
                .unwrap();

            let second_args = second_up_receiver.recv().await.unwrap();
            wait_for_flow_events(other_shards_alive_node_gossip_events)
                .await
                .unwrap();

            let second_node = create_metadata_from_args(
                second_args,
                number_of_shards_second_node,
            );

            for shard in &all_shards {
                assert_eq!(shard.nodes.borrow().len(), 1);
                assert_eq!(
                    shard.nodes.borrow().get(&second_node.name).unwrap(),
                    &second_node
                );
            }

            first_test_done_sender.send(()).await.unwrap();
            wait_for_flow_events(all_second_node_dead_events)
                .await
                .unwrap();

            for shard in &all_shards {
                assert!(shard.nodes.borrow().is_empty());
            }
        },
    )?;

    let seed_nodes = seed_receiver.recv_blocking()?;
    second_args.seed_nodes = seed_nodes;
    second_args.remote_shard_port += number_of_shards_first_node;
    second_args.port += number_of_shards_first_node;
    second_args.gossip_port += number_of_shards_first_node;
    second_args.name = "second".to_string();

    let second_handle = test_node(
        number_of_shards_second_node.into(),
        second_args.clone(),
        move |node_shard, other_shards| async move {
            let mut all_shards = other_shards.clone();
            all_shards.push(node_shard.clone());

            second_up_sender.send(second_args).await.unwrap();

            let first_node =
                create_metadata_from_args(args, number_of_shards_first_node);

            for shard in &all_shards {
                assert_eq!(shard.nodes.borrow().len(), 1);
                assert_eq!(
                    shard.nodes.borrow().get(&first_node.name).unwrap(),
                    &first_node
                );
            }

            first_test_done_receiver.recv().await.unwrap();
        },
    )?;

    second_handle.join()?;
    first_handle.join()?;

    Ok(())
}
