//! Minimal value and error types shared by `f4kvs-lsm`, `f4kvs-v2`, and `f4kvs-ffi`.

#![deny(missing_docs)]

mod error;
mod value;

pub use error::{ErrorSeverity, F4KvsError, Result};
pub use value::Value;