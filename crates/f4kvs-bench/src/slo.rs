use crate::harness::ResultType;
use std::time::Duration;

/// SLO thresholds for the trimmed benchmark (see docs/f4kvs-redb-benchmark-plan in projects-tracker).
#[derive(Clone, Copy, Debug)]
pub struct SloConfig {
    /// Maximum duration for the random-read phase (50k point lookups).
    pub max_random_reads: Duration,
    /// Maximum duration for O(1) `len()` at ~5M keys.
    pub max_len: Duration,
    /// Maximum wall-clock time for the full trimmed run.
    pub max_total: Duration,
}

impl Default for SloConfig {
    fn default() -> Self {
        Self {
            max_random_reads: Duration::from_secs(5),
            max_len: Duration::from_millis(10),
            max_total: Duration::from_secs(15 * 60),
        }
    }
}

#[derive(Debug)]
pub struct SloViolation {
    pub phase: String,
    pub observed: Duration,
    pub limit: Duration,
}

/// Check benchmark results against SLO gates. Returns all violations.
pub fn check_results(
    results: &[(String, ResultType)],
    total_elapsed: Duration,
    config: SloConfig,
) -> Vec<SloViolation> {
    let mut violations = Vec::new();

    for (phase, result) in results {
        let (limit, label) = match phase.as_str() {
            "random reads" => (config.max_random_reads, phase.clone()),
            "len()" => (config.max_len, phase.clone()),
            _ => continue,
        };
        if let ResultType::Duration(d) = result {
            if *d > limit {
                violations.push(SloViolation {
                    phase: label,
                    observed: *d,
                    limit,
                });
            }
        }
    }

    if total_elapsed > config.max_total {
        violations.push(SloViolation {
            phase: "total wall time".to_string(),
            observed: total_elapsed,
            limit: config.max_total,
        });
    }

    violations
}