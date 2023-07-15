use std::rc::Rc;

use dbeel::error::Result;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::net::TcpStream;
use rmpv::{decode::read_value_ref, encode::write_value, Value, ValueRef};
use test_utils::test_on_shard;

async fn send_create_collection(
    collection_name: &str,
    address: &(String, u16),
) -> std::io::Result<()> {
    let map = Value::Map(vec![
        (
            Value::String("type".into()),
            Value::String("create_collection".into()),
        ),
        (
            Value::String("name".into()),
            Value::String(collection_name.into()),
        ),
    ]);
    let mut data_encoded: Vec<u8> = Vec::new();
    write_value(&mut data_encoded, &map).unwrap();

    let mut stream = TcpStream::connect(address).await.unwrap();

    let size_buffer = (data_encoded.len() as u16).to_le_bytes();
    stream.write_all(&size_buffer).await?;
    stream.write_all(&data_encoded).await?;

    let mut response_buffer = Vec::new();
    stream.read_to_end(&mut response_buffer).await?;

    let response = read_value_ref(&mut &response_buffer[..]).unwrap();
    if response != ValueRef::String("OK".into()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Response not OK: {}", response),
        ));
    }

    Ok(())
}

#[test]
fn clean_state() -> Result<()> {
    test_on_shard(|shard| async move {
        assert!(shard.trees.borrow().is_empty());
    })
}

#[test]
fn find_collections_after_rerun() -> Result<()> {
    test_on_shard(|shard| async move {
        send_create_collection(
            "test",
            &(shard.args.ip.clone(), shard.args.port),
        )
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
