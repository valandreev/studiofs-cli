use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use studiofs_meta::Ino;

#[derive(Debug)]
pub struct QosController {
    accept_slots: Arc<Semaphore>,
    materialize_slots: Arc<Semaphore>,
    upload_slots: Arc<Semaphore>,
    commit_slots: Arc<Semaphore>,
    fetch_slots: Arc<Semaphore>,
}

impl QosController {
    pub fn from_env() -> Self {
        fn env_usize(key: &str, default: usize) -> usize {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default)
        }

        Self {
            accept_slots: Arc::new(Semaphore::new(env_usize("STUDIOFS_QOS_ACCEPT_SLOTS", 64))),
            materialize_slots: Arc::new(Semaphore::new(env_usize(
                "STUDIOFS_QOS_MATERIALIZE_SLOTS",
                2,
            ))),
            upload_slots: Arc::new(Semaphore::new(env_usize("STUDIOFS_QOS_UPLOAD_SLOTS", 4))),
            commit_slots: Arc::new(Semaphore::new(env_usize("STUDIOFS_QOS_COMMIT_SLOTS", 4))),
            fetch_slots: Arc::new(Semaphore::new(env_usize("STUDIOFS_QOS_FETCH_SLOTS", 128))),
        }
    }

    pub async fn acquire_accept(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.accept_slots.clone().acquire_owned().await
    }

    pub fn try_acquire_accept(&self) -> Option<OwnedSemaphorePermit> {
        self.accept_slots.clone().try_acquire_owned().ok()
    }

    pub async fn acquire_materialize(
        &self,
    ) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.materialize_slots.clone().acquire_owned().await
    }

    pub async fn acquire_upload(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.upload_slots.clone().acquire_owned().await
    }

    pub async fn acquire_commit(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.commit_slots.clone().acquire_owned().await
    }

    pub async fn acquire_fetch(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.fetch_slots.clone().acquire_owned().await
    }

    pub fn accept_available(&self) -> usize {
        self.accept_slots.available_permits()
    }

    pub fn materialize_available(&self) -> usize {
        self.materialize_slots.available_permits()
    }

    pub fn upload_available(&self) -> usize {
        self.upload_slots.available_permits()
    }

    pub fn commit_available(&self) -> usize {
        self.commit_slots.available_permits()
    }

    pub fn fetch_available(&self) -> usize {
        self.fetch_slots.available_permits()
    }
}

#[derive(Debug)]
pub struct FairInodeScheduler<T> {
    per_inode: BTreeMap<Ino, VecDeque<T>>,
    ready_inodes: VecDeque<Ino>,
    ready_set: HashSet<Ino>,
}

impl<T> Default for FairInodeScheduler<T> {
    fn default() -> Self {
        Self {
            per_inode: BTreeMap::new(),
            ready_inodes: VecDeque::new(),
            ready_set: HashSet::new(),
        }
    }
}

impl<T> FairInodeScheduler<T> {
    pub fn push(&mut self, ino: Ino, item: T) {
        self.per_inode.entry(ino).or_default().push_back(item);
        if self.ready_set.insert(ino) {
            self.ready_inodes.push_back(ino);
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        while let Some(ino) = self.ready_inodes.pop_front() {
            self.ready_set.remove(&ino);
            let mut remove_inode = false;
            let item = {
                let queue = self.per_inode.get_mut(&ino)?;
                let item = queue.pop_front();
                if queue.is_empty() {
                    remove_inode = true;
                } else {
                    self.ready_inodes.push_back(ino);
                    self.ready_set.insert(ino);
                }
                item
            };

            if remove_inode {
                self.per_inode.remove(&ino);
            }

            if item.is_some() {
                return item;
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.per_inode.is_empty()
    }

    pub fn len(&self) -> usize {
        self.per_inode.values().map(VecDeque::len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::FairInodeScheduler;

    #[test]
    fn round_robin_across_inodes() {
        let mut fair = FairInodeScheduler::default();
        fair.push(10, 1);
        fair.push(10, 2);
        fair.push(20, 3);
        fair.push(20, 4);
        fair.push(30, 5);

        let mut out = Vec::new();
        while let Some(item) = fair.pop() {
            out.push(item);
        }

        assert_eq!(out, vec![1, 3, 5, 2, 4]);
    }
}
