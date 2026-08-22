//! Packed, sorted SST index.
//!
//! On-disk layout (`F4IX` v1):
//!   magic[4] = b"F4IX"
//!   version u8 = 1
//!   count u32 LE
//!   rec_off[count] u32 LE  — byte offset of each record in `data`
//!   data: repeating { key_len u32 LE, key, file_off u64 LE, size u32 LE }
//!
//! Legacy files are a bincode `BTreeMap<String,(u64,u32)>`; [`FlatIndex::decode`]
//! accepts both. New writes always emit `F4IX`.

use crate::error::{LsmError, Result};
use std::collections::BTreeMap;

const MAGIC: &[u8; 4] = b"F4IX";
const VERSION: u8 = 1;

/// In-memory SST index. Keys stay packed; lookup is binary search.
#[derive(Debug, Clone, Default)]
pub struct FlatIndex {
    data: Vec<u8>,
    rec_off: Vec<u32>,
}

impl FlatIndex {
    pub fn len(&self) -> usize {
        self.rec_off.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rec_off.is_empty()
    }

    pub fn from_sorted(pairs: impl IntoIterator<Item = (String, u64, u32)>) -> Self {
        let pairs: Vec<(String, u64, u32)> = pairs.into_iter().collect();
        let mut data = Vec::with_capacity(pairs.len().saturating_mul(32));
        let mut rec_off = Vec::with_capacity(pairs.len());
        for (key, file_off, size) in pairs {
            rec_off.push(data.len() as u32);
            let kb = key.as_bytes();
            data.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            data.extend_from_slice(kb);
            data.extend_from_slice(&file_off.to_le_bytes());
            data.extend_from_slice(&size.to_le_bytes());
        }
        Self { data, rec_off }
    }

    pub fn get(&self, key: &str) -> Option<(u64, u32)> {
        let i = self.partition_point(key.as_bytes());
        let (k, loc) = self.at(i)?;
        if k == key.as_bytes() {
            Some(loc)
        } else {
            None
        }
    }

    /// First index whose key is >= `key`.
    pub fn partition_point(&self, key: &[u8]) -> usize {
        self.rec_off
            .partition_point(|&off| self.key_at_off(off as usize) < key)
    }

    pub fn at(&self, i: usize) -> Option<(&[u8], (u64, u32))> {
        if i >= self.rec_off.len() {
            return None;
        }
        Some(self.record_at_off(self.rec_off[i] as usize))
    }

    fn key_at_off(&self, off: usize) -> &[u8] {
        self.record_at_off(off).0
    }

    fn record_at_off(&self, off: usize) -> (&[u8], (u64, u32)) {
        let klen = u32::from_le_bytes(self.data[off..off + 4].try_into().unwrap()) as usize;
        let key = &self.data[off + 4..off + 4 + klen];
        let loc = off + 4 + klen;
        let file_off = u64::from_le_bytes(self.data[loc..loc + 8].try_into().unwrap());
        let size = u32::from_le_bytes(self.data[loc + 8..loc + 12].try_into().unwrap());
        (key, (file_off, size))
    }

    pub fn encode(&self) -> Vec<u8> {
        let n = self.rec_off.len() as u32;
        let mut out = Vec::with_capacity(9 + self.rec_off.len() * 4 + self.data.len());
        out.extend_from_slice(MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&n.to_le_bytes());
        for off in &self.rec_off {
            out.extend_from_slice(&off.to_le_bytes());
        }
        out.extend_from_slice(&self.data);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() >= 5 && buf.starts_with(MAGIC) {
            return decode_v1(buf);
        }
        decode_bincode(buf)
    }
}

fn decode_v1(buf: &[u8]) -> Result<FlatIndex> {
    if buf[4] != VERSION {
        return Err(LsmError::Serialization(format!(
            "unsupported F4IX version {}",
            buf[4]
        )));
    }
    if buf.len() < 9 {
        return Err(LsmError::Serialization("truncated F4IX header".into()));
    }
    let count = u32::from_le_bytes(buf[5..9].try_into().unwrap()) as usize;
    let table = 9;
    let records = table + count * 4;
    if buf.len() < records {
        return Err(LsmError::Serialization(
            "truncated F4IX offset table".into(),
        ));
    }
    let mut rec_off = Vec::with_capacity(count);
    for i in 0..count {
        let o = table + i * 4;
        rec_off.push(u32::from_le_bytes(buf[o..o + 4].try_into().unwrap()));
    }
    Ok(FlatIndex {
        data: buf[records..].to_vec(),
        rec_off,
    })
}

fn decode_bincode(buf: &[u8]) -> Result<FlatIndex> {
    let map: BTreeMap<String, (u64, u32)> = bincode::deserialize(buf)
        .map_err(|e| LsmError::Serialization(format!("Failed to deserialize index: {e}")))?;
    Ok(FlatIndex::from_sorted(
        map.into_iter().map(|(k, (off, sz))| (k, off, sz)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_lookup_and_legacy() {
        let idx = FlatIndex::from_sorted([
            ("a".into(), 10, 4),
            ("abc".into(), 20, 8),
            ("z".into(), 30, 2),
        ]);
        assert_eq!(idx.get("abc"), Some((20, 8)));
        assert_eq!(idx.get("nope"), None);
        let encoded = idx.encode();
        let decoded = FlatIndex::decode(&encoded).unwrap();
        assert_eq!(decoded.get("a"), Some((10, 4)));
        assert_eq!(decoded.get("z"), Some((30, 2)));

        let mut legacy: BTreeMap<String, (u64, u32)> = BTreeMap::new();
        legacy.insert("abc".into(), (20u64, 8u32));
        let raw = bincode::serialize(&legacy).unwrap();
        let from_legacy = FlatIndex::decode(&raw).unwrap();
        assert_eq!(from_legacy.get("abc"), Some((20, 8)));
    }
}
