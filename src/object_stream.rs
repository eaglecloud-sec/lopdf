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
        Self::new_with_limits_and_object_budget_and_abort_check(
            stream,
            max_decompressed_size,
            cumulative_budget,
            None,
            abort_check,
        )
    }

    pub fn new_with_limits_and_object_budget_and_abort_check(
        stream: &mut Stream, max_decompressed_size: Option<usize>,
        cumulative_budget: Option<(&std::sync::atomic::AtomicUsize, usize)>,
        object_budget: Option<(&std::sync::atomic::AtomicUsize, usize)>,
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

        let n = stream.dict.get(b"N").and_then(Object::as_i64)?;
        if let Some((_, limit)) = object_budget {
            if n < 0 || n as usize > limit {
                return Err(Error::ObjectLimitExceeded { limit });
            }
        }

        let numbers_str = std::str::from_utf8(index_block)?;
        let mut numbers = Vec::new();
        for number in numbers_str.split_whitespace() {
            if abort_check.map(|check| check()).unwrap_or(false) {
                return Err(Error::Aborted);
            }
            numbers.push(u32::from_str(number).ok());
            if let Some((_, limit)) = object_budget {
                if numbers.len() > limit.saturating_mul(2) {
                    return Err(Error::ObjectLimitExceeded { limit });
                }
            }
        }
        let len = numbers.len() / 2 * 2; // Ensure only pairs.

        if numbers.len().try_into().ok() != n.checked_mul(2) {
            warn!("object stream: the object stream dictionary specifies a wrong number of objects")
        }
        if let Some((used, limit)) = object_budget {
            let object_count = len / 2;
            let result = used.fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |current| current.checked_add(object_count).filter(|total| *total <= limit),
            );
            if result.is_err() {
                return Err(Error::ObjectLimitExceeded { limit });
            }
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

    use crate::{Error, Stream};

    use super::ObjectStream;

    #[test]
    fn cumulative_decompression_budget_is_enforced() {
        let mut stream = Stream::new(dictionary! { "N" => 0, "First" => 0 }, vec![b' '; 5]);
        let used = AtomicUsize::new(0);
        let err =
            ObjectStream::new_with_limits_and_abort_check(&mut stream, Some(1024), Some((&used, 4)), None).unwrap_err();
        assert!(matches!(err, Error::CumulativeDecompressionLimitExceeded { limit: 4 }));
    }

    #[test]
    fn cumulative_object_budget_is_enforced_before_object_materialization() {
        let used = AtomicUsize::new(0);
        let mut first = Stream::new(dictionary! { "N" => 1, "First" => 4 }, b"1 0 null".to_vec());
        ObjectStream::new_with_limits_and_object_budget_and_abort_check(
            &mut first,
            Some(1024),
            None,
            Some((&used, 1)),
            None,
        )
        .unwrap();

        let mut second = Stream::new(dictionary! { "N" => 1, "First" => 4 }, b"2 0 null".to_vec());
        let err = ObjectStream::new_with_limits_and_object_budget_and_abort_check(
            &mut second,
            Some(1024),
            None,
            Some((&used, 1)),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, Error::ObjectLimitExceeded { limit: 1 }));
    }
}
