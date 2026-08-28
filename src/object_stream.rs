#![cfg(any(feature = "pom_parser", feature = "nom_parser"))]

use crate::parser;
use crate::{Error, Object, ObjectId, Result, Stream};
use std::collections::BTreeMap;
use std::str::FromStr;

use log::warn;
#[cfg(feature = "rayon")]
use rayon::prelude::*;

#[derive(Debug)]
pub struct ObjectStream {
    pub objects: BTreeMap<ObjectId, Object>,
}

impl ObjectStream {
    pub fn new(stream: &mut Stream) -> Result<ObjectStream> {
        Self::new_with_limit(stream, None)
    }

    pub fn new_with_limit(stream: &mut Stream, max_decompressed_size: Option<usize>) -> Result<ObjectStream> {
        Self::new_with_limit_and_abort_check(stream, max_decompressed_size, None)
    }

    pub fn new_with_limit_and_abort_check(
        stream: &mut Stream, max_decompressed_size: Option<usize>, abort_check: Option<&(dyn Fn() -> bool + Sync)>,
    ) -> Result<ObjectStream> {
        Self::new_with_limits_and_abort_check(stream, max_decompressed_size, None, abort_check)
    }

    pub fn new_with_limits_and_abort_check(
        stream: &mut Stream, max_decompressed_size: Option<usize>,
        cumulative_budget: Option<(&std::sync::atomic::AtomicUsize, usize)>,
        abort_check: Option<&(dyn Fn() -> bool + Sync)>,
    ) -> Result<ObjectStream> {
        match max_decompressed_size {
            Some(max) => stream.decompress_with_limit(max)?,
            None => stream.decompress(),
        }
        if let Some((used, limit)) = cumulative_budget {
            crate::parser_aux::claim_cumulative_decompressed_bytes(used, stream.content.len(), limit)?;
        }

        if stream.content.is_empty() {
            return Ok(ObjectStream {
                objects: BTreeMap::new(),
            });
        }

        let first_offset = stream
            .dict
            .get(b"First")
            .and_then(Object::as_i64)?
            .try_into()
            .map_err(|_| Error::Offset(0))?;
        let index_block = stream.content.get(..first_offset).ok_or(Error::Offset(first_offset))?;

        let numbers_str = std::str::from_utf8(index_block)?;
        let mut numbers = Vec::new();
        for number in numbers_str.split_whitespace() {
            if abort_check.map(|check| check()).unwrap_or(false) {
                return Err(Error::Aborted);
            }
            numbers.push(u32::from_str(number).ok());
        }
        let len = numbers.len() / 2 * 2; // Ensure only pairs.

        let n = stream.dict.get(b"N").and_then(Object::as_i64)?;
        if numbers.len().try_into().ok() != n.checked_mul(2) {
            warn!("object stream: the object stream dictionary specifies a wrong number of objects")
        }

        let aborted = std::sync::atomic::AtomicBool::new(false);
        let chunks_filter_map = |chunk: &[_]| {
            if abort_check.map(|check| check()).unwrap_or(false) {
                aborted.store(true, std::sync::atomic::Ordering::Relaxed);
                return None;
            }
            let id = chunk[0]?;
            let offset = first_offset + chunk[1]? as usize;

            if offset >= stream.content.len() {
                warn!("out-of-bounds offset in object stream");
                return None;
            }
            let object = parser::direct_object(&stream.content[offset..])?;

            Some(((id, 0), object))
        };
        #[cfg(feature = "rayon")]
        let objects = numbers[..len].par_chunks(2).filter_map(chunks_filter_map).collect();
        #[cfg(not(feature = "rayon"))]
        let objects = numbers[..len].chunks(2).filter_map(chunks_filter_map).collect();

        if aborted.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(Error::Aborted);
        }

        Ok(ObjectStream { objects })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use crate::{dictionary, Error, Stream};

    use super::ObjectStream;

    #[test]
    fn cumulative_decompression_budget_is_enforced() {
        let mut stream = Stream::new(dictionary! { "N" => 0, "First" => 0 }, vec![b' '; 5]);
        let used = AtomicUsize::new(0);
        let err =
            ObjectStream::new_with_limits_and_abort_check(&mut stream, Some(1024), Some((&used, 4)), None).unwrap_err();
        assert!(matches!(err, Error::CumulativeDecompressionLimitExceeded { limit: 4 }));
    }
}
