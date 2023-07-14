use dbeel::{
    args::parse_args,
    error::{Error, Result},
    local_shard::LocalShardConnection,
    run_shard::{create_shard, run_shard},
};
use glommio::{enclose, CpuSet, LocalExecutorBuilder, Placement};
use pretty_env_logger::formatted_timed_builder;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

const DEFAULT_DEBUG_LOG_LEVEL: &str = "dbeel=trace";
const DEFAULT_RELEASE_LOG_LEVEL: &str = "dbeel=info";

fn main() -> Result<()> {
    let default_log_level = if cfg!(debug_assertions) {
        DEFAULT_DEBUG_LOG_LEVEL
    } else {
        DEFAULT_RELEASE_LOG_LEVEL
    };

    let mut log_builder = formatted_timed_builder();
    log_builder.parse_filters(
        &std::env::var("RUST_LOG")
            .unwrap_or_else(|_| default_log_level.to_string()),
    );
    log_builder.try_init().unwrap();

    let args = parse_args();

    let cpu_set = CpuSet::online()?;
    assert!(!cpu_set.is_empty());

    let local_connections = cpu_set
        .iter()
        .map(|x| x.cpu)
        .map(LocalShardConnection::new)
        .collect::<Vec<_>>();

    let handles = cpu_set
        .into_iter()
        .map(|x| x.cpu)
        .enumerate()
        .map(|(i, cpu)| {
            LocalExecutorBuilder::new(Placement::Fixed(cpu))
                .name(format!("executor({})", cpu).as_str())
                .spawn(enclose!((local_connections.clone() => connections,
                                args.clone() => args) move || async move {
                    let shard = create_shard(args, cpu, connections);
                    if let Err(e) = run_shard(shard, i == 0).await {
                        error!("Failed to start shard {}: {}", cpu, e);
                    }
                }))
                .map_err(Error::GlommioError)
        })
        .collect::<Result<Vec<_>>>()?;

    handles
        .into_iter()
        .map(|h| h.join().map_err(Error::GlommioError))
        .collect::<Result<Vec<_>>>()?;

    Ok(())
}
