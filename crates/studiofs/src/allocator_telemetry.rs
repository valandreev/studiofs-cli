use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicU64, Ordering};

use studiofs_vfs::VfsPortableAllocatorStats;

pub struct TrackingMiMalloc;

static CURRENT_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static REALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);

fn update_peak(candidate: u64) {
    let mut observed = PEAK_BYTES.load(Ordering::Acquire);
    while candidate > observed {
        match PEAK_BYTES.compare_exchange_weak(
            observed,
            candidate,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(next) => observed = next,
        }
    }
}

fn record_alloc(size: usize) {
    let size = size as u64;
    ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_TOTAL_BYTES.fetch_add(size, Ordering::Relaxed);
    let current = CURRENT_BYTES.fetch_add(size, Ordering::AcqRel) + size;
    update_peak(current);
}

fn record_dealloc(size: usize) {
    let size = size as u64;
    DEALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    DEALLOCATED_TOTAL_BYTES.fetch_add(size, Ordering::Relaxed);
    let _ = CURRENT_BYTES.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(size))
    });
}

fn record_realloc(old_size: usize, new_size: usize) {
    let old_size = old_size as u64;
    let new_size = new_size as u64;
    REALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_TOTAL_BYTES.fetch_add(new_size, Ordering::Relaxed);
    DEALLOCATED_TOTAL_BYTES.fetch_add(old_size, Ordering::Relaxed);
    let current = CURRENT_BYTES
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            Some(current.saturating_sub(old_size).saturating_add(new_size))
        })
        .unwrap_or_default()
        .saturating_sub(old_size)
        .saturating_add(new_size);
    update_peak(current);
}

unsafe impl GlobalAlloc for TrackingMiMalloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegates the allocation request unchanged to mimalloc's GlobalAlloc impl.
        let ptr = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegates the allocation request unchanged to mimalloc's GlobalAlloc impl.
        let ptr = unsafe { mimalloc::MiMalloc.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record_dealloc(layout.size());
        // SAFETY: Pointer/layout are provided by Rust's allocator contract.
        unsafe { mimalloc::MiMalloc.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: Delegates the reallocation request unchanged to mimalloc's GlobalAlloc impl.
        let new_ptr = unsafe { mimalloc::MiMalloc.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            record_realloc(layout.size(), new_size);
        }
        new_ptr
    }
}

pub fn snapshot() -> VfsPortableAllocatorStats {
    VfsPortableAllocatorStats {
        current_bytes: CURRENT_BYTES.load(Ordering::Acquire),
        peak_bytes: PEAK_BYTES.load(Ordering::Acquire),
        allocated_total_bytes: ALLOCATED_TOTAL_BYTES.load(Ordering::Acquire),
        deallocated_total_bytes: DEALLOCATED_TOTAL_BYTES.load(Ordering::Acquire),
        allocation_count: ALLOCATION_COUNT.load(Ordering::Acquire),
        deallocation_count: DEALLOCATION_COUNT.load(Ordering::Acquire),
        reallocation_count: REALLOCATION_COUNT.load(Ordering::Acquire),
    }
}
