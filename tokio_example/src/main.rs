use clap::Parser;
use dbeel_client::DbeelClient;
use rmpv::{decode::read_value_ref, Value};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
/// Tokio dbeel client example.
struct Args {
    #[clap(
        short,
        long,
        help = "Server hostname / ip.",
        default_value = "127.0.0.1"
    )]
    ip: String,

    #[clap(short, long, help = "Server port.", default_value = "10000")]
    port: u16,
}

const COLLECTION_NAME: &str = "tokio";

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let seed_nodes = [(args.ip.clone(), args.port)];
    let client = DbeelClient::from_seed_nodes(&seed_nodes).await.unwrap();
    let collection = client.create_collection(COLLECTION_NAME).await.unwrap();

    // Wait for creation of collection to get through all shards.
    // TODO: Wait in server for all local shards to respond and only then return.
    sleep(Duration::from_millis(100)).await;

    let value = Value::String("value".into());
    collection
        .set_from_str_key("key", value.clone())
        .await
        .unwrap();

    let response_buffer = collection.get_from_str_key("key").await.unwrap();
    let response = read_value_ref(&mut &response_buffer[..]).unwrap();
    assert_eq!(response, value.as_ref());

    collection.drop().await.unwrap();
}