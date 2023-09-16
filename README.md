<img src="./logo.svg">

## Introduction
dbeel is an attempt to learn modern database architecture.

It's not production ready at all, but that doesn't mean there is no value in the project.
If you ever wanted to read database code without getting overwhelmed by massive amounts of code, dbeel is for you.

## Traits
* LSM Tree
  * Memtable is a red black tree
* Thread per core
* io_uring
* Direct I/O
  * Page cache implemented using WTiny-LFU eviction algorithm
* Distributed events using gossip dissemination
* Leaderless replication with tunable consistency
  * `--replication-factor` - Number of nodes that will store a copy of data
  * Write `consistency` (parameter in `set` command) - Number of nodes that will acknowledge a write for it to succeed
  * Read `consistency` (parameter in `get` command) - Number of nodes that have to respond to a read operation for it to succeed
    * Max timestamp conflict resolution

## Try it out

To compile the DB:
``` sh
cargo build --release
```

To compile the blackbox benchmarks:
``` sh
cd blackbox_bench
cargo build --release
```

To run the benchmarks:

``` sh
./target/release/dbeel               # On first terminal
./target/release/blackbox_bench      # On second terminal
```

I have also written a mini client that you can use inside of python:

``` python
from dbeel import DB

document = {"hello": "world"}

assert DB.create("test") == "OK"
assert DB.set("test", "key", document) == "OK"
assert DB.get("test", "key") == document
assert DB.delete("test", "key") == "OK"
assert "key not found" in DB.get("test", "key")
assert DB.drop("test") == "OK"
```

## Performance
Running the benchmark on my machine ([System76 lemp11](https://tech-docs.system76.com/models/lemp11/README.html)) with no fdatasync results in the following output:

```
Set:
total: 48.733705959s, min: 106.442µs, p50: 347.806µs, p90: 928.311µs, p99: 1.751205ms, p999: 7.331712ms, max: 44.204281ms

Get:
total: 61.39613906s, min: 69.37µs, p50: 520.323µs, p90: 1.082239ms, p99: 1.813001ms, p999: 3.394344ms, max: 9.267451ms
```

Meaning the median user will have ~3 writes in a millisecond and ~2 reads in a millisecond.

Running with fdatasync results in the following output for Set:

```
Set:
total: 1278.977625712s, min: 4.097672ms, p50: 12.56982ms, p90: 12.895054ms, p99: 15.352365ms, p999: 91.415289ms, max: 324.257944ms
```

P90 / P99 are very important to me personally, because usually the most demanding users are the ones getting tail latencies, and they are the most important customer as they probably also use the service the most.
