use clap::Parser;
use dbeel_client::{Collection, DbeelClient};
use futures::future::join_all;
use glommio::{
    spawn_local, CpuLocation, CpuSet, LocalExecutorBuilder, Placement,
};
use rand::{seq::SliceRandom, thread_rng};
use rmpv::{decode::read_value_ref, Value, ValueRef};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
/// Benchmark the database.
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

    #[clap(
        long,
        help = "Number of async tasks per core.",
        default_value = "1"
    )]
    tasks: usize,

    #[clap(long, help = "Don't drop the created collection at the end.")]
    dont_drop: bool,
}

const COLLECTION_NAME: &str = "dbeel";

async fn set_request(
    collection: &Collection,
    client_index: usize,
    request_index: usize,
) -> Option<Duration> {
    let key = format!("{}_{}", client_index, request_index);
    let key_str = key.as_str();

    let start_time = Instant::now();

    match collection
        .set_from_str_key(key_str, Value::String(key_str.into()))
        .await
    {
        Ok(response_buffer) => {
            let response = read_value_ref(&mut &response_buffer[..]).unwrap();
            if response != ValueRef::String("OK".into()) {
                eprintln!("Response not OK: {}", response);
                return None;
            }
        }
        Err(e) => {
            eprintln!("Failed to receive response: {}", e);
            return None;
        }
    }

    Some(Instant::now().duration_since(start_time))
}

async fn get_request(
    collection: &Collection,
    client_index: usize,
    request_index: usize,
) -> Option<Duration> {
    let key = format!("{}_{}", client_index, request_index);
    let key_str = key.as_str();

    let start_time = Instant::now();

    match collection.get_from_str_key(key_str).await {
        Ok(response_buffer) => {
            let response = read_value_ref(&mut &response_buffer[..]).unwrap();
            if response != ValueRef::String(key_str.into()) {
                eprintln!("Response not OK: {}", response);
                return None;
            }
        }
        Err(e) => {
            eprintln!("Failed to receive response: {}", e);
            return None;
        }
    }

    Some(Instant::now().duration_since(start_time))
}

async fn run_benchmark(
    collection: Arc<Collection>,
    num_clients: usize,
    num_requests: usize,
    num_tasks: usize,
    set: bool,
) -> Vec<(usize, Vec<Duration>)> {
    let mut handles = Vec::new();

    let cpus: Vec<CpuLocation> =
        CpuSet::online().unwrap().into_iter().collect();
    let cpus_len = cpus.len();

    for client_index in 0..num_clients {
        let executor = LocalExecutorBuilder::new(Placement::Fixed(
            client_index % cpus_len,
        ));

        let collection = collection.clone();
        let handle = executor
            .name(format!("client-{}", client_index).as_str())
            .spawn(move || async move {
                let mut indices: Vec<usize> = (0..num_requests).collect();
                indices.shuffle(&mut thread_rng());

                let mut tasks = Vec::with_capacity(num_tasks);
                for request_indices in indices.chunks(num_requests / num_tasks)
                {
                    let chunk = request_indices.to_vec();
                    let collection = collection.clone();
                    tasks.push(spawn_local(async move {
                        let mut stats = Vec::with_capacity(chunk.len());

                        for request_index in chunk {
                            let maybe_duration = if set {
                                set_request(
                                    &collection,
                                    client_index,
                                    request_index,
                                )
                                .await
                            } else {
                                get_request(
                                    &collection,
                                    client_index,
                                    request_index,
                                )
                                .await
                            };

                            if let Some(duration) = maybe_duration {
                                stats.push(duration);
                            }
                        }

                        stats
                    }));
                }

                let results = join_all(tasks).await;
                results.into_iter().flatten().collect::<Vec<_>>()
            })
            .unwrap();

        handles.push((client_index, handle));
    }

    handles
        .into_iter()
        .map(|(i, handle)| (i, handle.join().unwrap()))
        .collect()
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

fn main() {
    let args = Args::parse();

    let builder = LocalExecutorBuilder::new(Placement::Unbound);
    let handle = builder
        .name("bb-bench")
        .spawn(move || async move {
            let address = (args.ip.clone(), args.port);

            let seed_nodes = [address.clone()];
            let client =
                DbeelClient::from_seed_nodes(&seed_nodes).await.unwrap();
            let collection = Arc::new(
                client.create_collection(COLLECTION_NAME).await.unwrap(),
            );

            let set_results = run_benchmark(
                collection.clone(),
                args.clients,
                args.requests,
                args.tasks,
                true,
            )
            .await;

            let get_results = run_benchmark(
                collection.clone(),
                args.clients,
                args.requests,
                args.tasks,
                false,
            )
            .await;

            if !args.dont_drop {
                if let Err(e) = Arc::<Collection>::into_inner(collection)
                    .unwrap()
                    .drop()
                    .await
                {
                    eprintln!("Failed to drop collection: {}", e);
                }
            }

            (set_results, get_results)
        })
        .unwrap();
    let (set_stats, get_stats) = handle.join().unwrap();

    println!("Set:");
    print_stats(set_stats);

    println!();

    println!("Get:");
    print_stats(get_stats);
}
