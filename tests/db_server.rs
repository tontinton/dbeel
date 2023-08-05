use std::sync::Once;

use dbeel::{
    args::{parse_args_from, Args},
    error::{Error, Result},
    tasks::db_server::ResponseError,
};
use dbeel_client::DbeelClient;
use rmp_serde::from_slice;
use rmpv::{decode::read_value_ref, Value, ValueRef};
use rstest::{fixture, rstest};
use serial_test::serial;
use test_utils::{install_logger, test_shard};

const ASSERT_AMOUNT_OF_TIMES: usize = 3;

static ONCE: Once = Once::new();

fn response_equals_error(response: Vec<u8>, e: Error) -> Result<bool> {
    let error: ResponseError = from_slice(&response)?;
    Ok(error.same_as(&e))
}

fn response_ok(response: Vec<u8>) -> Result<bool> {
    let value = read_value_ref(&mut &response[..])?;
    Ok(value == ValueRef::String("OK".into()))
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
        let collection = client.collection("test");
        let response = collection.get("non_existing_key").await.unwrap();
        assert!(response_equals_error(
            response,
            Error::CollectionNotFound("test".to_string())
        )
        .unwrap());
    })?;

    Ok(())
}

#[rstest]
#[serial]
fn drop_collection(args: Args) -> Result<()> {
    test_shard(args, |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        let collection =
            client.clone().create_collection("test").await.unwrap();
        collection.drop().await.unwrap();

        let collection = client.collection("test");
        let response = collection.get("non_existing_key").await.unwrap();
        assert!(response_equals_error(
            response,
            Error::CollectionNotFound("test".to_string())
        )
        .unwrap());
    })?;

    Ok(())
}

#[rstest]
#[serial]
fn get_non_existing_key(args: Args) -> Result<()> {
    test_shard(args, |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();
        let collection =
            client.clone().create_collection("test").await.unwrap();
        let response = collection.get("key").await.unwrap();
        assert!(response_equals_error(response, Error::KeyNotFound).unwrap());
    })?;

    Ok(())
}

#[rstest]
#[serial]
fn set_and_get_key(args: Args) -> Result<()> {
    test_shard(args, |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        let collection =
            client.clone().create_collection("test").await.unwrap();

        let response = collection.set("key", Value::F32(100.0)).await.unwrap();
        assert!(response_ok(response).unwrap());

        for _ in 0..ASSERT_AMOUNT_OF_TIMES {
            let response = collection.get("key").await.unwrap();
            let value = read_value_ref(&mut &response[..]).unwrap();
            assert_eq!(value, ValueRef::F32(100.0));
        }
    })?;

    Ok(())
}

#[rstest]
#[serial]
fn set_and_get_key_after_restart(args: Args) -> Result<()> {
    test_shard(args.clone(), |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        let collection =
            client.clone().create_collection("test").await.unwrap();

        let response = collection.set("key", Value::F32(100.0)).await.unwrap();
        assert!(response_ok(response).unwrap());
    })?;

    test_shard(args, |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        let collection = client.collection("test");
        for _ in 0..ASSERT_AMOUNT_OF_TIMES {
            let response = collection.get("key").await.unwrap();
            let value = read_value_ref(&mut &response[..]).unwrap();
            assert_eq!(value, ValueRef::F32(100.0));
        }
    })?;

    Ok(())
}

#[rstest]
#[serial]
fn delete_and_get_key(args: Args) -> Result<()> {
    test_shard(args, |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        let collection =
            client.clone().create_collection("test").await.unwrap();

        let response = collection.set("key", Value::F32(100.0)).await.unwrap();
        assert!(response_ok(response).unwrap());

        let response = collection.delete("key").await.unwrap();
        assert!(response_ok(response).unwrap());

        for _ in 0..ASSERT_AMOUNT_OF_TIMES {
            let response = collection.get("key").await.unwrap();
            assert!(
                response_equals_error(response, Error::KeyNotFound).unwrap()
            );
        }
    })?;

    Ok(())
}

#[rstest]
#[serial]
fn multiple_collections(args: Args) -> Result<()> {
    test_shard(args, |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        let collections = vec![
            client.clone().create_collection("test1").await.unwrap(),
            client.clone().create_collection("test2").await.unwrap(),
        ];

        for collection in &collections {
            let response =
                collection.set("key", Value::F32(100.0)).await.unwrap();
            assert!(response_ok(response).unwrap());
        }

        for collection in &collections {
            let response = collection.get("key").await.unwrap();
            let value = read_value_ref(&mut &response[..]).unwrap();
            assert_eq!(value, ValueRef::F32(100.0));
        }

        collections[0].delete("key").await.unwrap();

        let response = collections[0].get("key").await.unwrap();
        assert!(response_equals_error(response, Error::KeyNotFound).unwrap());
        let response = collections[1].get("key").await.unwrap();
        let value = read_value_ref(&mut &response[..]).unwrap();
        assert_eq!(value, ValueRef::F32(100.0));

        collections[1].delete("key").await.unwrap();

        for collection in &collections {
            let response = collection.get("key").await.unwrap();
            assert!(
                response_equals_error(response, Error::KeyNotFound).unwrap()
            );
        }
    })?;

    Ok(())
}
