//! Centralized formatting utilities for F4KVS storage

use std::time::Duration;

/// Convert bytes to human-readable format
///
/// This is the canonical implementation used across all F4KVS storage engines.
/// Previously duplicated in multiple locations, now centralized here.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

/// Convert duration to human-readable format
///
/// Provides consistent duration formatting across all storage engines.
pub fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    let millis = duration.subsec_millis();

    if hours > 0 {
        format!("{hours}h {minutes}m {seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds}s")
    } else if seconds > 0 {
        format!("{seconds}.{millis:03}s")
    } else {
        format!("{millis}ms")
    }
}

/// Format a percentage value with appropriate precision
pub fn format_percentage(value: f64, total: f64) -> String {
    if total == 0.0 {
        "0.00%".to_string()
    } else {
        let percentage = (value / total) * 100.0;
        if percentage == 0.0 {
            "0.00%".to_string()
        } else if percentage < 0.01 {
            "<0.01%".to_string()
        } else if percentage >= 100.0 {
            "100.00%".to_string()
        } else {
            format!("{:.2}%", percentage)
        }
    }
}

/// Format a rate (operations per second, bytes per second, etc.)
pub fn format_rate(count: u64, duration: Duration) -> String {
    if duration.is_zero() {
        "∞".to_string()
    } else {
        let rate = count as f64 / duration.as_secs_f64();
        if rate >= 1_000_000.0 {
            format!("{:.2}M ops/s", rate / 1_000_000.0)
        } else if rate >= 1_000.0 {
            format!("{:.2}K ops/s", rate / 1_000.0)
        } else {
            format!("{:.2} ops/s", rate)
        }
    }
}

/// Format a number with thousand separators (commas)
///
/// Converts a number to a string with commas inserted every three digits
/// from right to left. For example, 10000 becomes "10,000".
///
/// # Examples
///
/// ```
/// use f4kvs_storage_core::common::formatting::format_number_with_commas;
///
/// assert_eq!(format_number_with_commas(0), "0");
/// assert_eq!(format_number_with_commas(999), "999");
/// assert_eq!(format_number_with_commas(1000), "1,000");
/// assert_eq!(format_number_with_commas(10000), "10,000");
/// assert_eq!(format_number_with_commas(100000), "100,000");
/// ```
pub fn format_number_with_commas(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + (s.len() - 1) / 3);
    let chars: Vec<char> = s.chars().collect();

    for (i, &ch) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i).is_multiple_of(3) {
            result.push(',');
        }
        result.push(ch);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(1024_u64.pow(4)), "1.00 TB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_millis(100)), "100ms");
        assert_eq!(format_duration(Duration::from_secs(5)), "5.000s");
        assert_eq!(format_duration(Duration::from_secs(65)), "1m 5s");
        assert_eq!(format_duration(Duration::from_secs(3665)), "1h 1m 5s");
    }

    #[test]
    fn test_format_percentage() {
        assert_eq!(format_percentage(0.0, 100.0), "0.00%");
        assert_eq!(format_percentage(50.0, 100.0), "50.00%");
        assert_eq!(format_percentage(100.0, 100.0), "100.00%");
        assert_eq!(format_percentage(0.005, 100.0), "<0.01%");
        assert_eq!(format_percentage(0.01, 100.0), "0.01%");
    }

    #[test]
    fn test_format_rate() {
        assert_eq!(format_rate(1000, Duration::from_secs(1)), "1.00K ops/s");
        assert_eq!(format_rate(1500, Duration::from_secs(1)), "1.50K ops/s");
        assert_eq!(format_rate(2000000, Duration::from_secs(1)), "2.00M ops/s");
        assert_eq!(format_rate(100, Duration::from_secs(0)), "∞");
    }

    #[test]
    fn test_format_number_with_commas() {
        assert_eq!(format_number_with_commas(0usize), "0");
        assert_eq!(format_number_with_commas(999usize), "999");
        assert_eq!(format_number_with_commas(1000usize), "1,000");
        assert_eq!(format_number_with_commas(10000usize), "10,000");
        assert_eq!(format_number_with_commas(100000usize), "100,000");
        assert_eq!(format_number_with_commas(1000000usize), "1,000,000");
        assert_eq!(format_number_with_commas(1234567890usize), "1,234,567,890");
    }
}
