//! Common utilities and shared functionality across F4KVS storage engines
//!
//! This module consolidates utilities that were previously duplicated across
//! different storage engines, reducing code duplication and improving maintainability.

pub mod error_handling;
pub mod formatting;
pub mod io;
pub mod validation;

// Re-export commonly used utilities
pub use error_handling::*;
pub use formatting::*;
pub use io::*;
pub use validation::*;
