use std::{sync::Once, time::Duration};

use dbeel::{
    args::{parse_args_from, Args},
    error::Result,
};
use dbeel_client::DbeelClient;
use rstest::{fixture, rstest};
use serial_test::serial;
use test_utils::{install_logger, test_shard};

static ONCE: Once = Once::new();

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
#[serial]
fn clean_state(args: Args) -> Result<()> {
    test_shard(args, |shard| async move {
        assert!(shard.trees.borrow().is_empty());
    })
}

#[rstest]
#[serial]
fn find_collections_after_rerun(args: Args) -> Result<()> {
    test_shard(args.clone(), |shard| async move {
        let mut client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        client.set_read_timeout(Duration::from_secs(1));
        client.set_write_timeout(Duration::from_secs(1));
        client.create_collection("test").await.unwrap();

        assert_eq!(shard.trees.borrow().len(), 1);
        assert!(shard.trees.borrow().get(&"test".to_string()).is_some());
    })?;

    test_shard(args, |shard| async move {
        assert_eq!(shard.trees.borrow().len(), 1);
        assert!(shard.trees.borrow().get(&"test".to_string()).is_some());
    })?;

    Ok(())
}
