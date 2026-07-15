use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

/// Serializes FUSE open preparation and reply for one inode at a time.
#[derive(Debug, Default)]
pub struct FuseOpenReplySerializer {
    locks: Mutex<HashMap<u64, Arc<Mutex<()>>>>,
}

impl FuseOpenReplySerializer {
    /// Run one same-inode prepare-to-reply critical section.
    pub fn with_inode<R>(&self, ino: u64, section: impl FnOnce() -> R) -> R {
        let inode_lock = {
            let mut locks = self.locks.lock();
            Arc::clone(locks.entry(ino).or_insert_with(|| Arc::new(Mutex::new(()))))
        };
        let inode_guard = inode_lock.lock();
        let result = section();
        drop(inode_guard);

        let mut locks = self.locks.lock();
        if Arc::strong_count(&inode_lock) == 2
            && locks
                .get(&ino)
                .is_some_and(|current| Arc::ptr_eq(current, &inode_lock))
        {
            locks.remove(&ino);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use super::FuseOpenReplySerializer;

    const TIMEOUT: Duration = Duration::from_secs(1);

    #[test]
    fn same_inode_prepare_reply_sections_do_not_overlap() {
        let serializer = Arc::new(FuseOpenReplySerializer::default());
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let first_serializer = Arc::clone(&serializer);
        let first = std::thread::spawn(move || {
            first_serializer.with_inode(42, || {
                first_entered_tx.send(()).expect("signal first entry");
                release_first_rx.recv().expect("release first section");
            });
        });
        first_entered_rx
            .recv_timeout(TIMEOUT)
            .expect("first section enters");

        let (second_started_tx, second_started_rx) = mpsc::channel();
        let (second_entered_tx, second_entered_rx) = mpsc::channel();
        let second_serializer = Arc::clone(&serializer);
        let second = std::thread::spawn(move || {
            second_started_tx.send(()).expect("signal second attempt");
            second_serializer.with_inode(42, || {
                second_entered_tx.send(()).expect("signal second entry");
            });
        });
        second_started_rx
            .recv_timeout(TIMEOUT)
            .expect("second section attempts entry");
        assert!(
            second_entered_rx.recv_timeout(TIMEOUT).is_err(),
            "same-inode section entered before the first section completed"
        );

        release_first_tx.send(()).expect("release first section");
        second_entered_rx
            .recv_timeout(TIMEOUT)
            .expect("second section enters after release");
        first.join().expect("first section exits");
        second.join().expect("second section exits");
    }

    #[test]
    fn different_inode_prepare_reply_sections_can_progress_independently() {
        let serializer = Arc::new(FuseOpenReplySerializer::default());
        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let first_serializer = Arc::clone(&serializer);
        let first = std::thread::spawn(move || {
            first_serializer.with_inode(42, || {
                first_entered_tx.send(()).expect("signal first entry");
                release_first_rx.recv().expect("release first section");
            });
        });
        first_entered_rx
            .recv_timeout(TIMEOUT)
            .expect("first section enters");

        let (second_entered_tx, second_entered_rx) = mpsc::channel();
        let second_serializer = Arc::clone(&serializer);
        let second = std::thread::spawn(move || {
            second_serializer.with_inode(43, || {
                second_entered_tx.send(()).expect("signal second entry");
            });
        });
        second_entered_rx
            .recv_timeout(TIMEOUT)
            .expect("different inode must progress while first is held");

        release_first_tx.send(()).expect("release first section");
        first.join().expect("first section exits");
        second.join().expect("second section exits");
    }

    #[test]
    fn idle_inode_lock_entries_are_removed() {
        let serializer = FuseOpenReplySerializer::default();

        serializer.with_inode(42, || {});

        assert!(serializer.locks.lock().is_empty());
    }
}
