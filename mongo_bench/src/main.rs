use clap::Parser;
use mongodb::{
    bson::{doc, Document},
    options::ClientOptions,
    Client, Collection,
};
use rand::{seq::SliceRandom, thread_rng};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
/// Benchmark mongodb.
struct Args {
    #[clap(
        short,
        long,
        help = "Number of parallel connections.",
        default_value = "20"
    )]
    clients: usize,

    #[clap(
        short = 'n',
        long,
        help = "Total number of requests each client sends.",
        default_value = "5000"
    )]
    requests: usize,
}

const COLLECTION_NAME: &str = "dbeel";

async fn set_request(
    collection: &Collection<Document>,
    client_index: usize,
    request_index: usize,
) -> Option<Duration> {
    let key = format!("{}_{}", client_index, request_index);
    let key_str = key.as_str();

    let start_time = Instant::now();

    if let Err(e) = collection.insert_one(doc! {"_id": key_str}, None).await {
        eprintln!("Failed to receive response: {}", e);
        return None;
    }

    Some(Instant::now().duration_since(start_time))
}

async fn get_request(
    collection: &Collection<Document>,
    client_index: usize,
    request_index: usize,
) -> Option<Duration> {
    let key = format!("{}_{}", client_index, request_index);
    let key_str = key.as_str();

    let start_time = Instant::now();

    if let Err(e) = collection.find_one(Some(doc! {"_id": key_str}), None).await
    {
        eprintln!("Failed to receive response: {}", e);
        return None;
    }

    Some(Instant::now().duration_since(start_time))
}

async fn run_benchmark(
    collection: Arc<Collection<Document>>,
    num_clients: usize,
    num_requests: usize,
    set: bool,
) -> Vec<(usize, Vec<Duration>)> {
    let mut handles = Vec::new();

    for client_index in 0..num_clients {
        let collection = collection.clone();
        let handle = tokio::spawn(async move {
            let mut indices: Vec<usize> = (0..num_requests).collect();
            indices.shuffle(&mut thread_rng());

            let mut stats = Vec::with_capacity(indices.len());
            for request_index in indices {
                let maybe_duration = if set {
                    set_request(&collection, client_index, request_index).await
                } else {
                    get_request(&collection, client_index, request_index).await
                };

                if let Some(duration) = maybe_duration {
                    stats.push(duration);
                }
            }

            stats
        });

        handles.push((client_index, handle));
    }

    let mut result = Vec::with_capacity(handles.len());
    for (i, handle) in handles {
        let times = handle.await.unwrap();
        result.push((i, times));
    }
    result
}

fn print_stats(client_stats: Vec<(usize, Vec<Duration>)>) {
    if client_stats.is_empty() {
        return;
    }

    let mut stats: Vec<Duration> = client_stats
        .into_iter()
        .flat_map(|(_, stats)| stats)
        .collect();
    stats.sort();

    let total: Duration = stats.iter().sum();
    let last_index = stats.len() - 1;
    let min = stats[0];
    let max = stats[last_index];
    let p50 = stats[last_index / 2];
    let p90 = stats[last_index * 9 / 10];
    let p99 = stats[last_index * 99 / 100];
    let p999 = stats[last_index * 999 / 1000];
    println!(
        "total: {:?}, min: {:?}, p50: {:?}, p90: {:?}, p99: {:?}, p999: {:?}, max: {:?}",
        total, min, p50, p90, p99, p999, max
    );
}

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let args = Args::parse();

    let client_options =
        ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    let db = client.database("mydb");

    let collection = Arc::new(db.collection::<Document>(COLLECTION_NAME));

    let set_stats =
        run_benchmark(collection.clone(), args.clients, args.requests, true)
            .await;

    let get_stats =
        run_benchmark(collection.clone(), args.clients, args.requests, false)
            .await;

    println!("Set:");
    print_stats(set_stats);

    println!();

    println!("Get:");
    print_stats(get_stats);

    Ok(())
}
