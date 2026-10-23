//! Shared helpers for the parser/codec microbenchmarks.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

// --- RESP payload builders ---------------------------------------------------

pub fn bulk(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
}

pub fn simple(out: &mut Vec<u8>, data: &str) {
    out.extend_from_slice(format!("+{data}\r\n").as_bytes());
}

pub fn array_header(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(format!("*{len}\r\n").as_bytes());
}

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

/// A global allocator that counts allocations, used to quantify heap traffic.
pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Allocation counters captured at a point in time.
pub struct AllocSnapshot {
    count: usize,
    bytes: usize,
}

/// Captures the current allocation counters.
pub fn alloc_snapshot() -> AllocSnapshot {
    AllocSnapshot {
        count: ALLOC_COUNT.load(Ordering::Relaxed),
        bytes: ALLOC_BYTES.load(Ordering::Relaxed),
    }
}

impl AllocSnapshot {
    /// Returns `(allocations, bytes)` counted since the snapshot was taken.
    pub fn since(&self) -> (usize, usize) {
        (
            ALLOC_COUNT.load(Ordering::Relaxed) - self.count,
            ALLOC_BYTES.load(Ordering::Relaxed) - self.bytes,
        )
    }
}
