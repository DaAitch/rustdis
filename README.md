# Rustdis

A redis wannabe clone, with just a few commands focussing on performance, learning Tokio and performance profiling. It's an exercise.

## Run

```bash
# Debug (is way slower)
cargo run

# Release
cargo run --release
```

## Supported commands

- GET
- SET
- INFO (returns nothing but "OK")

## Current benchmarks

[benchmark readme](./benchmark/README.md)

- running official `redis-server` (Redis server v=5.0.7, currently latest on Ubuntu `apt install`)
- vs. running rustdis `cargo run --release`

Benchmark: `redis-benchmark -t set,get -n 1000000 -r 1000000 -c 1000`

- 1M requests for SET and GET
- 1M random values
- 1k concurrent banging clients

Current results (on my machine):

- rustdis: ~73k/s (~81% redis performance)
- redis-server: ~90k/s

## Perf / Flamegraph

### Install Flamegraph

```bash
cargo install flamegraph
```

### Linux Kernel perf events

```bash
# with sudo
echo "kernel.perf_event_paranoid = -1" >> /etc/sysctl.conf
sysctl --system
```

### Run

```bash
flamegraph ./target/release/rustdis
```

## Performance Logs

### Trial and error testing improvements:

1. 🆗 increase buffer and channel sizes
1. 🆗 make tokio channel unbound
1. 🆗 using `tokio::task::block_in_place` instead of "spawn and join"
1. 🆗 "bug" using `tokio::mpsc::channel` wrong with blocking thread
1. 🆗 move shared state access into `Processor`
1. 🆗 double-buffered read buffer 2-phase loop: 1. reading (empty buf), 2. sending/reading

### Flamegraph investigation:

1. 🔝 buffering socket writes to a phase (+8% req/s through-put)
1. 🔜 WIP: optimize chatty unbound channel with buffered payloads (according to the flamegraph this might be the next bottleneck)

### What didn't help (maybe will help at some time in the future):

- 🔙 tweak rust compilation (see Cargo.toml)
- 🔙 tokio increasing max blocking threads
- 🔙 checking out different sync primitives mutex/rwlock
- 🔙 key-partitioning sync primitives into 2, 4, 8, 128 parts

