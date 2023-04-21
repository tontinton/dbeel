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

## Performance
Running the benchmark results in the following output:

```
Set:
total: 49.739198791s, min: 258.539µs, p50: 1.587444ms, p90: 2.481965ms, p99: 14.063627ms, p999: 103.281282ms, max: 104.122006ms

Get:
total: 56.231811984s, min: 213.972µs, p50: 2.837968ms, p90: 3.723611ms, p99: 4.608398ms, p999: 23.587334ms, max: 27.51648ms
```

Meaning median write is 1.58ms and median read is 2.83ms.

P90 / P99 are very important to me personally, because usually the most demanding users are the ones getting tail latencies, and they are the most important customer as they probably also use the service the most.
