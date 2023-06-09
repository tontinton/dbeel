## Introduction
dbeel is an attempt to learn modern database architecture.

It's not production ready at all, but that doesn't mean there is no value in the project.
If you ever wanted to read database code without getting overwhelmed by massive amounts of code, dbeel is for you.

## Traits
* LSM Tree (Memtable is a red black tree)
* io_uring
* Direct I/O
* Custom page cache using WTiny-LFU eviction algorithm
* Thread per core
* Distributed using gossip dissemination

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
total: 8.808562092s, min: 91.967µs, p50: 358.538µs, p90: 769.608µs, p99: 1.308489ms, p999: 3.542049ms, max: 25.519163ms

Get:
total: 6.05312515s, min: 53.671µs, p50: 134.195µs, p90: 758.463µs, p99: 1.604895ms, p999: 2.925088ms, max: 5.121712ms
```

Meaning the median user will have ~3 writes in a millisecond and ~7 reads in a millisecond.

Running with fdatasync results in the following output for Set:

```
Set:
total: 267.588985462s, min: 6.344815ms, p50: 12.43828ms, p90: 13.273325ms, p99: 22.733286ms, p999: 267.005295ms, max: 319.146534ms
```

P90 / P99 are very important to me personally, because usually the most demanding users are the ones getting tail latencies, and they are the most important customer as they probably also use the service the most.
