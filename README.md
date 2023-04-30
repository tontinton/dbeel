## Introduction
dbil is an attempt to learn modern database architecture.

It's not production ready at all, but that doesn't mean there is no value in the project.
If you ever wanted to read database code without getting overwhelmed by massive amounts of code, dbil is for you.

## Traits
* LSM Tree (Memtable is a red black tree)
* io_uring
* Direct I/O
* Custom page cache using WTiny-LFU eviction algorithm
* Thread per core (currently only runs on 1 core :P)

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
BENCHMARK=1 ./target/release/dbil   # On first terminal
./target/release/blackbox_bench     # On second terminal
```

## Performance
Running the benchmark results in the following output:

```
Set:
total: 39.637963212s, min: 312.495µs, p50: 981.203µs, p90: 2.837719ms, p99: 8.379054ms, p999: 101.891049ms, max: 103.660656ms

Get:
total: 33.889400913s, min: 212.567µs, p50: 1.263362ms, p90: 3.128171ms, p99: 7.633867ms, p999: 9.755045ms, max: 17.755394ms
```

Meaning median write is 1.58ms and median read is 2.83ms.

P90 / P99 are very important to me personally, because usually the most demanding users are the ones getting tail latencies, and they are the most important customer as they probably also use the service the most.
