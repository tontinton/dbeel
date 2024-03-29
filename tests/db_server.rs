use std::sync::Once;

use dbeel::{
    args::{parse_args_from, Args},
    error::{Error, Result},
    tasks::db_server::ResponseError,
};
use dbeel_client::{self, DbeelClient};
use rmpv::Value;
use rstest::{fixture, rstest};
use serial_test::serial;
use test_utils::{install_logger, test_shard};

const ASSERT_AMOUNT_OF_TIMES: usize = 3;

static ONCE: Once = Once::new();

fn response_equals_error(
    response: dbeel_client::error::Error,
    error: &Error,
) -> bool {
    if let dbeel_client::error::Error::SendRequestToCluster(errors) = response {
        let re = ResponseError::new(error);
        errors.iter().all(|e| {
            if let dbeel_client::error::Error::ServerErr(name, message) = e {
                name == &re.name && message == &re.message
            } else {
                panic!("Expected server error");
            }
        })
    } else {
        panic!("Expected cluster request error");
    }
}

fn response_ok(value: Value) -> Result<bool> {
    Ok(value == Value::String("OK".into()))
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

        assert!(response_equals_error(
            client.collection("test").await.unwrap_err(),
            &Error::CollectionNotFound("test".to_string())
        ));
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

        let collection = client.create_collection("test").await.unwrap();
        collection.drop().await.unwrap();

        assert!(response_equals_error(
            client.collection("test").await.unwrap_err(),
            &Error::CollectionNotFound("test".to_string())
        ));
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
        let collection = client.create_collection("test").await.unwrap();
        let response = collection.get_from_str_key("key").await;
        assert!(response_equals_error(
            response.unwrap_err(),
            &Error::KeyNotFound,
        ));
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

        let collection = client.create_collection("test").await.unwrap();

        let response = collection
            .set_from_str_key("key", Value::F32(100.0))
            .await
            .unwrap();
        assert!(response_ok(response).unwrap());

        for _ in 0..ASSERT_AMOUNT_OF_TIMES {
            let value = collection.get_from_str_key("key").await.unwrap();
            assert_eq!(value, Value::F32(100.0));
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

        let collection = client.create_collection("test").await.unwrap();

        let response = collection
            .set_from_str_key("key", Value::F32(100.0))
            .await
            .unwrap();
        assert!(response_ok(response).unwrap());
    })?;

    test_shard(args, |shard| async move {
        let client = DbeelClient::from_seed_nodes(&[(
            shard.args.ip.clone(),
            shard.args.port,
        )])
        .await
        .unwrap();

        let collection = client.collection("test").await.unwrap();
        for _ in 0..ASSERT_AMOUNT_OF_TIMES {
            let value = collection.get_from_str_key("key").await.unwrap();
            assert_eq!(value, Value::F32(100.0));
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

        let collection = client.create_collection("test").await.unwrap();

        let response = collection
            .set_from_str_key("key", Value::F32(100.0))
            .await
            .unwrap();
        assert!(response_ok(response).unwrap());

        let response = collection.delete_from_str_key("key").await.unwrap();
        assert!(response_ok(response).unwrap());

        for _ in 0..ASSERT_AMOUNT_OF_TIMES {
            let response = collection.get_from_str_key("key").await;
            assert!(response_equals_error(
                response.unwrap_err(),
                &Error::KeyNotFound
            ));
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
            client.create_collection("test1").await.unwrap(),
            client.create_collection("test2").await.unwrap(),
        ];

        for collection in &collections {
            let response = collection
                .set_from_str_key("key", Value::F32(100.0))
                .await
                .unwrap();
            assert!(response_ok(response).unwrap());
        }

        for collection in &collections {
            let value = collection.get_from_str_key("key").await.unwrap();
            assert_eq!(value, Value::F32(100.0));
        }

        collections[0].delete_from_str_key("key").await.unwrap();

        let response = collections[0].get_from_str_key("key").await;
        assert!(response_equals_error(
            response.unwrap_err(),
            &Error::KeyNotFound
        ));

        let value = collections[1].get_from_str_key("key").await.unwrap();
        assert_eq!(value, Value::F32(100.0));

        collections[1].delete_from_str_key("key").await.unwrap();

        for collection in &collections {
            let response = collection.get_from_str_key("key").await;
            assert!(response_equals_error(
                response.unwrap_err(),
                &Error::KeyNotFound
            ));
        }
    })?;

    Ok(())
}
