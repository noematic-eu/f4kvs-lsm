//! Trimmed LMDB-style benchmark harness for f4kvs-lsm (redb-bench compatible subset).

mod adapter;
mod harness;
mod slo;

pub use adapter::F4kvsBenchDatabase;
pub use harness::{
    benchmark_trimmed, BenchmarkOptions, ResultType, TRIMMED_OPTIONS, KEY_SIZE,
};
pub use slo::{SloConfig, SloViolation, check_results};