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
  * `replication_factor` (parameter in `create_collection` command) - Number of nodes that will store a copy of data
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
Running the benchmark on my machine ([System76 lemp11](https://tech-docs.system76.com/models/lemp11/README.html)) with no `fdatasync` results in the following output:

```
Set:
total: 42.987212632s, min: 97.298µs, p50: 375.032µs, p90: 716.749µs, p99: 1.25213ms, p999: 1.965492ms, max: 12.534306ms

Get:
total: 61.716233278s, min: 69.543µs, p50: 533.423µs, p90: 1.081879ms, p99: 1.733866ms, p999: 2.231455ms, max: 4.155056ms
```

Meaning the median user will have ~3 writes in a millisecond and ~2 reads in a millisecond.

Running with `--wal-sync` (`fdatasync`s after each write) results in the following output for Set (note that `fdatasync` on my machine takes 6-10ms):

```
Set:
total: 1253.611595658s, min: 6.625024ms, p50: 12.57609ms, p90: 12.858347ms, p99: 13.4931ms, p999: 19.062725ms, max: 31.880792ms
```

P90 / P99 are very important to me personally, because usually the most demanding users are the ones getting tail latencies, and they are the most important customer as they probably also use the service the most.
