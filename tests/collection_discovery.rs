use std::rc::Rc;

use dbeel::{
    args::parse_args_from,
    error::Result,
    flow_events::FlowEvent,
    local_shard::LocalShardConnection,
    run_shard::{create_shard, run_shard},
    shards::MyShard,
};
use futures::Future;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    enclose, net::TcpStream, spawn_local,
    LocalExecutorBuilder, Placement,
};
use rmpv::{decode::read_value_ref, encode::write_value, Value, ValueRef};

fn test_on_shard<G, F, T>(test_future: G) -> Result<()>
where
    G: FnOnce(Rc<MyShard>) -> F + Send + 'static,
    F: Future<Output = T> + 'static,
    T: Send + 'static,
{
    let builder = LocalExecutorBuilder::new(Placement::Fixed(1));
    let handle = builder.name("test").spawn(|| async move {
        let args = parse_args_from([""]);
        let id = 0;
        let shard = create_shard(args, id, vec![LocalShardConnection::new(id)]);
        let start_event_receiver =
            shard.subscribe_to_flow_event(FlowEvent::StartTasks.into());
        let shard_run_handle =
            spawn_local(enclose!((shard.clone() => shard) async move {
                run_shard(shard, false).await
            }));
        start_event_receiver.recv().await.unwrap();

        // Test start
        test_future(shard).await;
        // Test end

        shard_run_handle.cancel().await;
    })?;
    handle.join()?;
    Ok(())
}

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
