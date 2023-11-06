use clap::Parser;
use dbeel_client::{Consistency, DbeelClient};
use rmpv::Value;

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
    let collection = client
        .create_collection_with_replication(COLLECTION_NAME, 3)
        .await?;

    let key = Value::String("key".into());
    let document = Value::Map(vec![
        (Value::String("is_best_db".into()), Value::Boolean(true)),
        (
            Value::String("owner".into()),
            Value::String("tontinton".into()),
        ),
    ]);

    collection
        .set_consistent(key.clone(), document.clone(), Consistency::Quorum)
        .await?;

    let response = collection.get_consistent(key, Consistency::Quorum).await?;
    assert_eq!(response, document);

    collection.drop().await?;

    Ok(())
}
