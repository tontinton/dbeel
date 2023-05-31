use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
/// A stupid database, by Tony Solomonik.
pub struct Args {
    #[clap(
        short,
        long,
        help = "Unique node name, used to differentiate between nodes for \
                distribution of load.",
        default_value = "dbeel"
    )]
    pub name: String,

    #[clap(
        short,
        long,
        help = "The seed nodes for service discovery of all nodes.
Expected format is <hostname/ip>:<port>.",
        num_args = 0..,
    )]
    pub seed_nodes: Vec<String>,

    #[clap(
        short,
        long,
        help = "Listen hostname / ip.",
        default_value = "127.0.0.1"
    )]
    pub ip: String,

    #[clap(
        short,
        long,
        help = "Server port base.
Each shard has a different port calculated by <port_base> + \
                <cpu_id>.
This port is for listening on client requests.",
        default_value = "10000"
    )]
    pub port: u16,

    #[clap(
        short,
        long,
        help = "Database files directory.",
        default_value = "/tmp"
    )]
    pub dir: String,

    #[clap(
        long,
        help = "Remote shard port base.
This port is for listening for distributed messages from \
                remote shards.",
        default_value = "20000"
    )]
    pub remote_shard_port: u16,

    #[clap(
        long,
        help = "Remote shard connect timeout in milliseconds.",
        default_value = "5000"
    )]
    pub remote_shard_connect_timeout: u64,

    #[clap(
        long,
        help = "Gossip UDP server port.
This port is for listening for gossip messages from \
                remote nodes.",
        default_value = "30000"
    )]
    pub gossip_port: u16,

    #[clap(long, help = "Gossip number of nodes fanout.", default_value = "3")]
    pub gossip_fanout: usize,

    #[clap(
        long,
        help = "Gossip max number of items an event is seen before \
retransmitting the event again.",
        default_value = "3"
    )]
    pub gossip_max_seen_count: u8,

    #[clap(
        long,
        help = "The interval at which to ping a node, in milliseconds.",
        default_value = "500"
    )]
    pub failure_detection_interval: u64,

    #[clap(
        short,
        long,
        help = "How much files to compact each time.",
        default_value = "2"
    )]
    pub compaction_factor: usize,

    #[clap(
        long,
        help = "Page cache size in bytes.",
        default_value = "1073741824"
    )]
    pub page_cache_size: usize,
}

pub fn get_args() -> Args {
    Args::parse()
}
