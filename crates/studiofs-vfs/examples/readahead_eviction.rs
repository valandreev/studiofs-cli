//! Benchmarks readahead eviction under concurrent access.

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use studiofs_vfs::readahead::{ReadAheadPolicy, ReadAheadTracker, ReadStreamKey};

const INODE_COUNT: u64 = 16_384;
const STREAMS_PER_INODE: u64 = 4;
const READER_THREADS: usize = 4;
const BENCHMARK_WINDOW: Duration = Duration::from_millis(250);

fn benchmark_policy() -> ReadAheadPolicy {
    ReadAheadPolicy {
        base_width: 4,
        max_width: 8,
        min_prefetch_on_seq: 4,
        prefetch_enabled: true,
    }
}

fn seed_tracker(tracker: &ReadAheadTracker) {
    let policy = benchmark_policy();
    for ino in 1..=INODE_COUNT {
        for stream_id in 0..STREAMS_PER_INODE {
            let key = ReadStreamKey { ino, stream_id };
            let _ = tracker.record_read_for_stream(key, 0, 0, policy);
            let _ = tracker.record_read_for_stream(key, 1, 1, policy);
        }
        assert!(tracker.start_prefetch(ino, ino as u32));
    }
}

fn main() {
    let tracker = Arc::new(ReadAheadTracker::new_with_bounds(4, 8, 4));
    seed_tracker(&tracker);

    let start_gate = Arc::new(Barrier::new(READER_THREADS + 1));
    let mut readers = Vec::with_capacity(READER_THREADS);
    for reader_index in 0..READER_THREADS {
        let tracker = Arc::clone(&tracker);
        let start_gate = Arc::clone(&start_gate);
        readers.push(thread::spawn(move || {
            let policy = benchmark_policy();
            start_gate.wait();
            let deadline = Instant::now() + BENCHMARK_WINDOW;
            let mut operations = 0_u64;
            while Instant::now() < deadline {
                let ino = 2 + (operations + reader_index as u64) % (INODE_COUNT - 1);
                let block = (operations % 1024) as u32;
                let key = ReadStreamKey {
                    ino,
                    stream_id: 1000 + reader_index as u64,
                };
                black_box(tracker.record_read_for_stream(key, block, block, policy));
                if tracker.start_prefetch(ino, block) {
                    tracker.finish_prefetch(ino, block);
                }
                operations += 1;
            }
            operations
        }));
    }

    start_gate.wait();
    let target = ReadStreamKey {
        ino: 1,
        stream_id: 0,
    };
    let policy = benchmark_policy();
    let benchmark_deadline = Instant::now() + BENCHMARK_WINDOW;
    let mut evictions = 0_u64;
    let mut eviction_time = Duration::ZERO;
    while Instant::now() < benchmark_deadline {
        let started = Instant::now();
        tracker.evict(target.ino);
        eviction_time += started.elapsed();
        evictions += 1;
        black_box(tracker.record_read_for_stream(target, 0, 0, policy));
    }

    let foreground_operations: u64 = readers
        .into_iter()
        .map(|reader| reader.join().expect("reader thread should finish"))
        .sum();
    let eviction_time_us = eviction_time.as_secs_f64() * 1_000_000.0;
    let per_eviction_us = eviction_time_us / evictions.max(1) as f64;

    println!("inodes={INODE_COUNT}");
    println!("streams_per_inode={STREAMS_PER_INODE}");
    println!("reader_threads={READER_THREADS}");
    println!("foreground_operations={foreground_operations}");
    println!("evictions={evictions}");
    println!("eviction_total_us={eviction_time_us:.2}");
    println!("eviction_per_call_us={per_eviction_us:.4}");
}
