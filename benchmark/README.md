# Benchmarks for rustdis

## Install

*Notice: on MacOS `redis-benchmark` is checking some config, which is not yet implemented by rustdis, so it doesn't work.*

- install `cargo` / Rust
- install `redis-benchmark`

```bash
npm ci
```

## Run benchmarks

```bash
# whole benchmark (all iterations)
npm run bench

# short benchmark (one iteration)
RUSTDISBENCH_SHORT=true npm run bench
```
