use std::path::Path;
use std::time::{Duration, Instant};

pub const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

const BULK_ELEMENTS: usize = 5_000_000;
const INDIVIDUAL_WRITES: usize = 1_000;
const NOSYNC_WRITES: usize = 50_000;
const BATCH_WRITES: usize = 100;
const BATCH_SIZE: usize = 1000;

fn random_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    rng.fill(&mut key);
    let mut value = vec![0u8; VALUE_SIZE];
    rng.fill(&mut value);
    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

#[derive(Clone, Copy)]
pub struct BenchmarkOptions {
    pub read_iterations: usize,
    pub num_reads: usize,
    pub run_compaction: bool,
}

/// Same profile as redb-bench `F4KVS_TRIMMED_OPTIONS`.
pub const TRIMMED_OPTIONS: BenchmarkOptions = BenchmarkOptions {
    read_iterations: 1,
    num_reads: 50_000,
    run_compaction: true,
};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResultType {
    Duration(Duration),
    SizeInBytes(u64),
    NA,
}

impl std::fmt::Display for ResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use byte_unit::{Byte, UnitType};

        match self {
            ResultType::NA => write!(f, "N/A"),
            ResultType::Duration(d) => write!(f, "{}ms", d.as_millis()),
            ResultType::SizeInBytes(s) => {
                let b = Byte::from_u64(*s).get_appropriate_unit(UnitType::Binary);
                write!(f, "{b:.2}")
            }
        }
    }
}

pub trait BenchDatabase {
    type C<'db>: BenchDatabaseConnection
    where
        Self: 'db;

    fn db_type_name() -> &'static str;
    fn connect(&self) -> Self::C<'_>;
    fn compact(&mut self) -> bool {
        false
    }
}

pub trait BenchDatabaseConnection: Send {
    type W<'db>: BenchWriteTransaction
    where
        Self: 'db;
    type R<'db>: BenchReadTransaction
    where
        Self: 'db;

    fn set_sync(&mut self, _sync: bool) -> bool {
        false
    }
    fn write_transaction(&self) -> Self::W<'_>;
    fn read_transaction(&self) -> Self::R<'_>;
}

pub trait BenchWriteTransaction {
    type W<'txn>: BenchInserter
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_>;
    fn commit(self) -> Result<(), ()>;
}

pub trait BenchInserter {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;
    fn remove(&mut self, key: &[u8]) -> Result<(), ()>;
}

pub trait BenchReadTransaction {
    type T<'txn>: BenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_>;
}

#[allow(clippy::len_without_is_empty)]
pub trait BenchReader {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;

    fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>>;
    fn len(&mut self) -> u64;
}

fn database_size(path: &Path) -> u64 {
    let mut size = 0u64;
    for entry in walkdir::WalkDir::new(path) {
        size += entry.expect("walkdir").metadata().expect("metadata").len();
    }
    size
}

fn nosync_writes<T: BenchDatabase + Send + Sync>(
    connection: &T::C<'_>,
    rng: &mut fastrand::Rng,
) -> ResultType {
    let start = Instant::now();
    for _ in 0..NOSYNC_WRITES {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        let (key, value) = random_pair(rng);
        inserter.insert(&key, &value).unwrap();
        drop(inserter);
        txn.commit().unwrap();
    }
    let duration = start.elapsed();
    println!(
        "{}: Wrote {} individual items in {}ms, with nosync",
        T::db_type_name(),
        NOSYNC_WRITES,
        duration.as_millis()
    );
    ResultType::Duration(duration)
}

/// Run the trimmed benchmark matrix (writes, 50k reads, compaction).
pub fn benchmark_trimmed<T: BenchDatabase + Send + Sync>(
    mut db: T,
    path: &Path,
    options: BenchmarkOptions,
) -> (Vec<(String, ResultType)>, Duration) {
    let total_start = Instant::now();
    let mut rng = make_rng();
    let mut results = Vec::new();
    let mut connection = db.connect();

    let start = Instant::now();
    {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..BULK_ELEMENTS {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }
    let duration = start.elapsed();
    println!(
        "{}: Bulk loaded {} items in {}ms",
        T::db_type_name(),
        BULK_ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load".to_string(), ResultType::Duration(duration)));

    let start = Instant::now();
    for _ in 0..INDIVIDUAL_WRITES {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        let (key, value) = random_pair(&mut rng);
        inserter.insert(&key, &value).unwrap();
        drop(inserter);
        txn.commit().unwrap();
    }
    let duration = start.elapsed();
    println!(
        "{}: Wrote {} individual items in {}ms",
        T::db_type_name(),
        INDIVIDUAL_WRITES,
        duration.as_millis()
    );
    results.push(("individual writes".to_string(), ResultType::Duration(duration)));

    let start = Instant::now();
    for _ in 0..BATCH_WRITES {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..BATCH_SIZE {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }
    let duration = start.elapsed();
    println!(
        "{}: Wrote {} batches of {} items in {}ms",
        T::db_type_name(),
        BATCH_WRITES,
        BATCH_SIZE,
        duration.as_millis()
    );
    results.push(("batch writes".to_string(), ResultType::Duration(duration)));

    if connection.set_sync(false) {
        results.push(("nosync writes".to_string(), nosync_writes::<T>(&connection, &mut rng)));
    } else {
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..NOSYNC_WRITES {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
        results.push(("nosync writes".to_string(), ResultType::NA));
    }
    connection.set_sync(true);

    let elements = BULK_ELEMENTS + INDIVIDUAL_WRITES + BATCH_SIZE * BATCH_WRITES + NOSYNC_WRITES;
    let txn = connection.read_transaction();
    {
        let start = Instant::now();
        let len = txn.get_reader().len();
        assert_eq!(len, elements as u64);
        let duration = start.elapsed();
        println!("{}: len() in {}ms", T::db_type_name(), duration.as_millis());
        results.push(("len()".to_string(), ResultType::Duration(duration)));
    }

    for _ in 0..options.read_iterations {
        let mut rng = make_rng();
        let start = Instant::now();
        let mut checksum = 0u64;
        let mut expected_checksum = 0u64;
        let mut reader = txn.get_reader();
        for _ in 0..options.num_reads {
            let (key, value) = random_pair(&mut rng);
            let result = reader.get(&key).unwrap();
            checksum += result.as_ref()[0] as u64;
            expected_checksum += value[0] as u64;
        }
        assert_eq!(checksum, expected_checksum);
        let duration = start.elapsed();
        println!(
            "{}: Random read {} items in {}ms",
            T::db_type_name(),
            options.num_reads,
            duration.as_millis()
        );
        results.push(("random reads".to_string(), ResultType::Duration(duration)));
    }
    drop(txn);

    results.push(("random range reads".to_string(), ResultType::NA));
    for threads in [4, 8, 16, 32] {
        results.push((
            format!("random reads ({threads} threads)"),
            ResultType::NA,
        ));
    }
    for phase in ["removals", "retain", "extract_if", "pop"] {
        results.push((phase.to_string(), ResultType::NA));
    }

    let uncompacted_size = database_size(path);
    results.push((
        "uncompacted size".to_string(),
        ResultType::SizeInBytes(uncompacted_size),
    ));
    drop(connection);

    if options.run_compaction {
        let start = Instant::now();
        if db.compact() {
            let duration = start.elapsed();
            println!(
                "{}: Compacted in {}ms",
                T::db_type_name(),
                duration.as_millis()
            );
            {
                let connection = db.connect();
                let mut txn = connection.write_transaction();
                let mut inserter = txn.get_inserter();
                let (key, value) = random_pair(&mut rng);
                inserter.insert(&key, &value).unwrap();
                drop(inserter);
                txn.commit().unwrap();
            }
            results.push((
                "compacted size".to_string(),
                ResultType::SizeInBytes(database_size(path)),
            ));
        } else {
            results.push(("compacted size".to_string(), ResultType::NA));
        }
    } else {
        results.push(("compacted size".to_string(), ResultType::NA));
    }

    let total_elapsed = total_start.elapsed();
    (results, total_elapsed)
}