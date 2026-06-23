use f4kvs_bench::{
    benchmark_trimmed, check_results, F4kvsBenchDatabase, SloConfig, TRIMMED_OPTIONS,
};
use std::env;
use std::process;
use std::time::Duration;
use tempfile::TempDir;

fn parse_duration_ms(flag: &str, args: &mut impl Iterator<Item = String>) -> Option<Duration> {
    let value = args.next()?;
    value
        .parse::<u64>()
        .ok()
        .map(Duration::from_millis)
        .or_else(|| {
            eprintln!("invalid value for {flag}: {value}");
            None
        })
}

fn parse_args() -> SloConfig {
    let mut config = SloConfig::default();
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--max-random-reads-ms" => {
                if let Some(d) = parse_duration_ms("--max-random-reads-ms", &mut args) {
                    config.max_random_reads = d;
                }
            }
            "--max-total-secs" => {
                if let Some(v) = args.next().and_then(|s| s.parse::<u64>().ok()) {
                    config.max_total = Duration::from_secs(v);
                }
            }
            "--help" | "-h" => {
                println!(
                    "f4kvs-trimmed-bench — redb-style trimmed harness with SLO gates\n\
                     \n\
                     Options:\n\
                       --max-random-reads-ms MS   random-read phase limit (default: 5000)\n\
                       --max-total-secs SECS      full run wall limit (default: 900)\n"
                );
                process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other} (try --help)");
                process::exit(2);
            }
        }
    }
    config
}

fn main() {
    let slo = parse_args();
    let tmpdir = TempDir::new().expect("tempdir");
    let path = tmpdir.path();

    println!("Running f4kvs trimmed benchmark (50k reads, WAL off)...");
    println!(
        "SLO gates: random reads <= {}ms, total <= {}s",
        slo.max_random_reads.as_millis(),
        slo.max_total.as_secs()
    );

    let db = F4kvsBenchDatabase::new(path);
    let (results, measured_total) = benchmark_trimmed(db, path, TRIMMED_OPTIONS);

    println!();
    for (name, result) in &results {
        println!("{name}: {result}");
    }
    println!();
    println!("total wall time: {}ms", measured_total.as_millis());

    let violations = check_results(&results, measured_total, slo);
    if violations.is_empty() {
        println!("SLO: PASS");
        return;
    }

    eprintln!("SLO: FAIL");
    for v in &violations {
        eprintln!(
            "  {} — observed {:?}ms, limit {:?}ms",
            v.phase,
            v.observed.as_millis(),
            v.limit.as_millis()
        );
    }
    process::exit(1);
}