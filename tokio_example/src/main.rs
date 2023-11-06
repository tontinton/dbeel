use clap::Parser;
use dbeel_client::{Consistency, DbeelClient};
use rmpv::{decode::read_value_ref, Value};

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
async fn main() -> dbeel_client::error::Result<()> {
    let args = Args::parse();

    let seed_nodes = [(args.ip.clone(), args.port)];
    let client = DbeelClient::from_seed_nodes(&seed_nodes).await?;
    let collection = client.create_collection(COLLECTION_NAME).await?;

    let key = Value::String("key".into());
    let value = Value::String("value".into());

    collection
        .set_consistent(key.clone(), value.clone(), Consistency::Quorum)
        .await?;

    let response_buffer =
        collection.get_consistent(key, Consistency::Quorum).await?;
    let response = read_value_ref(&mut &response_buffer[..])?;
    assert_eq!(response, value.as_ref());

    collection.drop().await?;

    Ok(())
}
