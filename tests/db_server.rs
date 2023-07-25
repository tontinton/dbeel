use std::sync::Once;

use dbeel::{
    args::{parse_args_from, Args},
    error::{Error, Result},
};
use dbeel_client::DbeelClient;
use rmpv::{decode::read_value_ref, ValueRef};
use rstest::{fixture, rstest};
use serial_test::serial;
use test_utils::{install_logger, test_shard};

static ONCE: Once = Once::new();

fn response_contains_error(response: Vec<u8>, e: Error) -> bool {
    if let Ok(ValueRef::String(response_str)) =
        read_value_ref(&mut &response[..])
    {
        response_str
            .into_str()
            .unwrap()
            .contains(format!("{}", e).as_str())
    } else {
        panic!("Not a string.");
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
#[serial]
fn get_non_existing_collection(args: Args) -> Result<()> {
    test_shard(args, |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();
        let collection = client.collection("test".into());
        let response = collection.get("non_existing_key").await.unwrap();
        assert!(response_contains_error(
            response,
            Error::CollectionNotFound("test".to_string())
        ));
    })?;

    Ok(())
}
