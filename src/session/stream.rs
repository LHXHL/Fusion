use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Debug, Default)]
pub struct StreamIdAllocator {
    next_id: AtomicU32,
}

impl StreamIdAllocator {
    pub fn new(start: u32) -> Self {
        Self {
            next_id: AtomicU32::new(start),
        }
    }

    pub fn next(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::StreamIdAllocator;

    #[test]
    fn allocator_increments_monotonically() {
        let alloc = StreamIdAllocator::new(1);
        assert_eq!(alloc.next(), 1);
        assert_eq!(alloc.next(), 2);
        assert_eq!(alloc.next(), 3);
    }
}
