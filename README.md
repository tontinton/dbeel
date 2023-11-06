<img src="./logo.svg">

## Introduction
dbeel is an attempt to learn modern database architecture.

It's not production ready at all, but that doesn't mean there is no value in the project.
If you ever wanted to read database code without getting overwhelmed by massive amounts of code, dbeel is for you.

## Traits
* Stores documents in `msgpack` format
* LSM Tree
  * Memtable is a red black tree
* Thread per core (thanks `glommio`)
* io_uring (thanks again `glommio`)
* Direct I/O
  * Page cache implemented using WTiny-LFU eviction algorithm
* Distributed events using gossip dissemination
* Leaderless replication with tunable consistency
  * `replication_factor` (parameter in `create_collection` command) - Number of nodes that will store a copy of data
  * Write `consistency` (parameter in `set` command) - Number of nodes that will acknowledge a write for it to succeed
  * Read `consistency` (parameter in `get` command) - Number of nodes that have to respond to a read operation for it to succeed
    * Max timestamp conflict resolution

## Performance
Running the benchmark on my machine ([System76 lemp11](https://tech-docs.system76.com/models/lemp11/README.html)) with no `fdatasync` results in the following output:

```
Set:
total: 54.424290449s, min: 80.219µs, p50: 446.851µs, p90: 905.422µs, p99: 1.806261ms, p999: 7.463916ms, max: 35.385961ms

Get:
total: 29.281556369s, min: 36.577µs, p50: 231.464µs, p90: 479.929µs, p99: 1.222589ms, p999: 3.269881ms, max: 6.242454ms
```

Running with `--wal-sync` (calls `fdatasync` after each write to the WAL file) results in the following output for Set (note that `fdatasync` on my machine takes 6-10ms):

```
Set:
total: 1253.611595658s, min: 6.625024ms, p50: 12.57609ms, p90: 12.858347ms, p99: 13.4931ms, p999: 19.062725ms, max: 31.880792ms
```

You can always configure `--wal-sync` to achieve better throughput, with worse tail latencies, by setting `--wal-sync-delay` (try setting half the time it takes to `fdatasync` a file on average in your setup).

## How to use
The only implemented client is in async rust, and can work on either `glommio` or `tokio` (select which using cargo features).

Documents are formatted in `msgpack` and the best crate I found for it is `rmpv`, so the client makes heavy use of it.

Example (mostly copied from `tokio_example/`):

```rust
// When connecting to a cluster, you provide nodes to request cluster metadata from.
let seed_nodes = [("127.0.0.1", 10000)];
let client = DbeelClient::from_seed_nodes(&seed_nodes).await?;

// Create a collection with replication of 3 (meaning 3 copies for each document).
let collection = client.create_collection_with_replication(COLLECTION_NAME, 3).await?;

// Create key and document using rmpv.
let key = Value::String("key".into());
let document = Value::Map(vec![
    (Value::String("is_best_db".into()), Value::Boolean(true)),
    (Value::String("owner".into()), Value::String("tontinton".into())),
]);

// Write document using quorum consistency.
collection.set_consistent(key.clone(), value.clone(), Consistency::Quorum).await?;

// Read document using quorum consistency.
let response = collection.get_consistent(key, Consistency::Quorum).await?;
assert_eq!(response, value);

// Drop collection.
collection.drop().await?;
```

## Try it out
To compile the DB:
``` sh
cargo build --release
./target/release/dbeel --help
```

To compile the blackbox benchmarks:
``` sh
cd blackbox_bench
cargo build --release
```

To run the benchmarks:

``` sh
./target/release/dbeel               # On first terminal
./target/release/blackbox-bench      # On second terminal
```
