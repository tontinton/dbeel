use clap::Parser;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    net::TcpStream, CpuLocation, CpuSet, LocalExecutorBuilder, Placement,
};
use rmpv::{decode::read_value_ref, encode::write_value, Value, ValueRef};
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
/// Benchmark the database.
struct Args {
    #[clap(
        short,
        long,
        help = "Server hostname (default %v)",
        default_value = "127.0.0.1"
    )]
    hostname: String,

    #[clap(
        short,
        long,
        help = "Server port (default %v)",
        default_value = "10000"
    )]
    port: u16,

    #[clap(
        short,
        long,
        help = "Number of parallel connections (default %v)",
        default_value = "20"
    )]
    clients: usize,

    #[clap(
        short = 'n',
        long,
        help = "Total number of requests each client sends (default %v)",
        default_value = "1000"
    )]
    requests: usize,
}

async fn run_benchmark(
    address: (String, u16),
    num_clients: usize,
    num_requests: usize,
) -> Vec<(usize, Vec<Duration>)> {
    let mut handles = Vec::new();

    let cpus: Vec<CpuLocation> =
        CpuSet::online().unwrap().into_iter().collect();

    for client_index in 0..num_clients {
        let executor = LocalExecutorBuilder::new(Placement::Fixed(
            cpus[client_index % cpus.len()].cpu,
        ));

        let address = address.clone();
        let handle = executor
            .name(format!("client-{}", client_index).as_str())
            .spawn(move || async move {
                let mut stats = Vec::new();

                for request_index in 0..num_requests {
                    let mut data_encoded: Vec<u8> = Vec::new();
                    write_value(
                        &mut data_encoded,
                        &Value::Map(vec![
                            (
                                Value::String("type".into()),
                                Value::String("set".into()),
                            ),
                            (
                                Value::String("key".into()),
                                Value::String(
                                    format!(
                                        "{}_{}",
                                        client_index, request_index
                                    )
                                    .into(),
                                ),
                            ),
                            (
                                Value::String("value".into()),
                                Value::String("value".into()),
                            ),
                        ]),
                    )
                    .unwrap();

                    let start_time = Instant::now();
                    let mut stream =
                        TcpStream::connect(&address).await.unwrap();

                    let size_buffer = (data_encoded.len() as u16).to_le_bytes();
                    if let Err(e) = stream.write_all(&size_buffer).await {
                        eprintln!("Failed to send size: {}", e);
                        continue;
                    }

                    if let Err(e) = stream.write_all(&data_encoded).await {
                        eprintln!("Failed to send data: {}", e);
                        continue;
                    }

                    let mut response_buffer = Vec::new();
                    if let Err(e) =
                        stream.read_to_end(&mut response_buffer).await
                    {
                        eprintln!("Failed to receive response: {}", e);
                        continue;
                    }

                    let response =
                        read_value_ref(&mut &response_buffer[..]).unwrap();
                    if response != ValueRef::String("OK".into()) {
                        eprintln!("Response not OK: {}", response);
                        continue;
                    }

                    stats.push(Instant::now().duration_since(start_time));
                }
                stats
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
    let mut stats: Vec<Duration> = client_stats
        .into_iter()
        .map(|(_, stats)| stats)
        .flatten()
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
        "total: {:?}, min: {:?}, p50: {:?}, p90: {:?}, p99: {:?}, p999: {:?} max: {:?}",
        total, min, p50, p90, p99, p999, max
    );
}

fn main() {
    let args = Args::parse();

    let builder = LocalExecutorBuilder::new(Placement::Unbound)
        .spin_before_park(Duration::from_millis(10));
    let handle = builder
        .name("bb-bench")
        .spawn(move || async move {
            run_benchmark(
                (args.hostname, args.port),
                args.clients,
                args.requests,
            )
            .await
        })
        .unwrap();
    let all_stats = handle.join().unwrap();
    print_stats(all_stats);
}
