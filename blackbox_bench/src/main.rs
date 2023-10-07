use clap::Parser;
use futures::future::join_all;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    net::TcpStream, spawn_local, timer::sleep, CpuLocation, CpuSet,
    LocalExecutorBuilder, Placement,
};
use murmur3::murmur3_32;
use rand::{seq::SliceRandom, thread_rng};
use rmpv::{decode::read_value_ref, encode::write_value, Value, ValueRef};
use std::time::{Duration, Instant};

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
        default_value = "2"
    )]
    tasks: usize,
}

const COLLECTION_NAME: &str = "dbeel";

async fn request(
    set: bool,
    cpus: usize,
    ip: &str,
    base_port: u16,
    client_index: usize,
    request_index: usize,
) -> Option<Duration> {
    let mut data_encoded: Vec<u8> = Vec::new();
    let key = format!("{}_{}", client_index, request_index);
    let key_str = key.as_str();

    let hash = murmur3_32(&mut std::io::Cursor::new(key_str), 0).unwrap();
    let port = base_port + (hash % (cpus as u32)) as u16;

    let request_type = if set { "set" } else { "get" };

    let mut parameters = vec![
        (
            Value::String("type".into()),
            Value::String(request_type.into()),
        ),
        (
            Value::String("collection".into()),
            Value::String(COLLECTION_NAME.into()),
        ),
        (Value::String("key".into()), Value::String(key_str.into())),
    ];
    if set {
        parameters.push((
            Value::String("value".into()),
            Value::String(key_str.into()),
        ));
    };
    let map = Value::Map(parameters);
    write_value(&mut data_encoded, &map).unwrap();

    let start_time = Instant::now();
    let mut stream = TcpStream::connect((ip, port)).await.unwrap();

    let size_buffer = (data_encoded.len() as u16).to_le_bytes();
    if let Err(e) = stream.write_all(&size_buffer).await {
        eprintln!("Failed to send size: {}", e);
        return None;
    }

    if let Err(e) = stream.write_all(&data_encoded).await {
        eprintln!("Failed to send data: {}", e);
        return None;
    }

    let mut response_buffer = Vec::new();
    if let Err(e) = stream.read_to_end(&mut response_buffer).await {
        eprintln!("Failed to receive response: {}", e);
        return None;
    }

    let response =
        read_value_ref(&mut &response_buffer[..response_buffer.len() - 1])
            .unwrap();
    if set {
        if response != ValueRef::String("OK".into()) {
            eprintln!("Response not OK: {}", response);
            return None;
        }
    } else if response != ValueRef::String(key_str.into()) {
        eprintln!("Response not OK: {}", response);
        return None;
    }

    Some(Instant::now().duration_since(start_time))
}

async fn run_benchmark(
    ip: String,
    base_port: u16,
    num_clients: usize,
    num_requests: usize,
    num_tasks: usize,
    set: bool,
) -> Vec<(usize, Vec<Duration>)> {
    let mut handles = Vec::new();

    let cpus: Vec<CpuLocation> =
        CpuSet::online().unwrap().into_iter().collect();
    let half_cpus = cpus.len() / 2;

    for client_index in 0..num_clients {
        let executor = LocalExecutorBuilder::new(Placement::Fixed(
            half_cpus + client_index % half_cpus,
        ));

        let ip = ip.clone();
        let handle = executor
            .name(format!("client-{}", client_index).as_str())
            .spawn(move || async move {
                let mut indices: Vec<usize> = (0..num_requests).collect();
                indices.shuffle(&mut thread_rng());

                let mut tasks = Vec::with_capacity(num_tasks);
                for request_indices in indices.chunks(num_requests / num_tasks)
                {
                    let chunk = request_indices
                        .into_iter()
                        .cloned()
                        .collect::<Vec<_>>();
                    let ip = ip.clone();
                    tasks.push(spawn_local(async move {
                        let mut stats = Vec::new();
                        for request_index in chunk {
                            if let Some(duration) = request(
                                set,
                                half_cpus,
                                &ip,
                                base_port,
                                client_index,
                                request_index,
                            )
                            .await
                            {
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

async fn send_collection_request(
    address: &(String, u16),
    request_type: &str,
) -> std::io::Result<()> {
    let map = Value::Map(vec![
        (
            Value::String("type".into()),
            Value::String(request_type.into()),
        ),
        (
            Value::String("name".into()),
            Value::String(COLLECTION_NAME.into()),
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

    let response =
        read_value_ref(&mut &response_buffer[..response_buffer.len() - 1])
            .unwrap();
    if response != ValueRef::String("OK".into()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Response not OK: {}", response),
        ));
    }

    Ok(())
}

async fn create_collection(address: &(String, u16)) -> std::io::Result<()> {
    send_collection_request(address, "create_collection").await
}

async fn drop_collection(address: &(String, u16)) -> std::io::Result<()> {
    send_collection_request(address, "drop_collection").await
}

fn print_stats(client_stats: Vec<(usize, Vec<Duration>)>) {
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
            create_collection(&address).await.unwrap();

            // Wait for creation of collection to get through all shards.
            sleep(Duration::from_millis(100)).await;

            let set_results = run_benchmark(
                address.0.clone(),
                address.1,
                args.clients,
                args.requests,
                args.tasks,
                true,
            )
            .await;

            let get_results = run_benchmark(
                address.0.clone(),
                address.1,
                args.clients,
                args.requests,
                args.tasks,
                false,
            )
            .await;

            if let Err(e) = drop_collection(&address).await {
                eprintln!("Failed to drop collection: {}", e);
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
