use crate::harness::ResultType;
use std::time::Duration;

/// SLO thresholds for the trimmed benchmark (see docs/f4kvs-redb-benchmark-plan in projects-tracker).
#[derive(Clone, Copy, Debug)]
pub struct SloConfig {
    /// Maximum duration for the random-read phase (50k point lookups).
    pub max_random_reads: Duration,
    /// Maximum wall-clock time for the full trimmed run.
    pub max_total: Duration,
}

impl Default for SloConfig {
    fn default() -> Self {
        Self {
            max_random_reads: Duration::from_secs(5),
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
        if phase == "random reads" {
            if let ResultType::Duration(d) = result {
                if *d > config.max_random_reads {
                    violations.push(SloViolation {
                        phase: phase.clone(),
                        observed: *d,
                        limit: config.max_random_reads,
                    });
                }
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