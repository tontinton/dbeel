use dbeel::error::Result;
use dbeel_client::create_collection;
use test_utils::test_on_shard;

#[test]
fn clean_state() -> Result<()> {
    test_on_shard(|shard| async move {
        assert!(shard.trees.borrow().is_empty());
    })
}

#[test]
fn find_collections_after_rerun() -> Result<()> {
    test_on_shard(|shard| async move {
        create_collection("test", &(shard.args.ip.clone(), shard.args.port))
            .await
            .unwrap();

        assert_eq!(shard.trees.borrow().len(), 1);
        assert!(shard.trees.borrow().get(&"test".to_string()).is_some());
    })?;

    test_on_shard(|shard| async move {
        assert_eq!(shard.trees.borrow().len(), 1);
        assert!(shard.trees.borrow().get(&"test".to_string()).is_some());
    })?;

    Ok(())
}
