use crate::{merge::BoxedIterator, value::InternalValue};
use std::collections::VecDeque;

/// Reads through a disjoint, sorted set of segment readers
pub struct MultiReader<'a> {
    readers: VecDeque<BoxedIterator<'a>>,
}

impl<'a> MultiReader<'a> {
    #[must_use]
    pub fn new(readers: VecDeque<BoxedIterator<'a>>) -> Self {
        Self { readers }
    }
}

impl<'a> Iterator for MultiReader<'a> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.readers.front_mut()?.next() {
                return Some(item);
            }

            // NOTE: Current reader has no more items, load next reader if it exists and try again
            self.readers.pop_front();
        }
    }
}

impl<'a> DoubleEndedIterator for MultiReader<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.readers.back_mut()?.next_back() {
                return Some(item);
            }

            // NOTE: Current reader has no more items, load next reader if it exists and try again
            self.readers.pop_back();
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{AbstractTree, Slice};
    use test_log::test;

    // TODO: same test for prefix & ranges

    #[test]
    fn segment_multi_reader_basic() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(&tempdir).open()?;

        let ids = [
            ["a", "b", "c"],
            ["d", "e", "f"],
            ["g", "h", "i"],
            ["j", "k", "l"],
        ];

        for batch in ids {
            for id in batch {
                tree.insert(id, vec![], 0);
            }
            tree.flush_active_memtable()?;
        }

        let segments = tree
            .levels
            .read()
            .expect("lock is poisoned")
            .iter()
            .collect::<Vec<_>>();

        #[allow(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<BoxedIterator<'_>> = VecDeque::new();

            for segment in &segments {
                readers.push_back(Box::new(segment.iter()));
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        }

        #[allow(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<BoxedIterator<'_>> = VecDeque::new();

            for segment in &segments {
                readers.push_back(Box::new(segment.iter()));
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.rev().flatten();

            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
        }

        #[allow(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<BoxedIterator<'_>> = VecDeque::new();

            for segment in &segments {
                readers.push_back(Box::new(segment.iter()));
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next_back().unwrap().key.user_key);
        }

        Ok(())
    }
}
