use std::io::{self, IsTerminal, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub(super) struct ProgressCounter {
    inner: Option<Arc<ProgressInner>>,
}

impl ProgressCounter {
    pub(super) fn inc(&self) {
        if let Some(inner) = &self.inner {
            inner.completed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(super) struct ProgressBar {
    inner: Option<Arc<ProgressInner>>,
    handle: Option<JoinHandle<()>>,
}

struct ProgressInner {
    label: String,
    total: usize,
    completed: AtomicUsize,
    done: AtomicBool,
    started: Instant,
}

impl ProgressBar {
    pub(super) fn start(label: impl Into<String>, total: usize) -> Self {
        let label = label.into();
        if total == 0 || !io::stderr().is_terminal() {
            eprintln!("{label}: started");
            return Self {
                inner: None,
                handle: None,
            };
        }

        let inner = Arc::new(ProgressInner {
            label,
            total,
            completed: AtomicUsize::new(0),
            done: AtomicBool::new(false),
            started: Instant::now(),
        });
        render(&inner);
        let thread_inner = inner.clone();
        let handle = std::thread::spawn(move || {
            while !thread_inner.done.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(250));
                render(&thread_inner);
            }
        });
        Self {
            inner: Some(inner),
            handle: Some(handle),
        }
    }

    pub(super) fn counter(&self) -> ProgressCounter {
        ProgressCounter {
            inner: self.inner.clone(),
        }
    }

    pub(super) fn finish(mut self) {
        if let Some(inner) = &self.inner {
            inner.completed.store(inner.total, Ordering::Relaxed);
            inner.done.store(true, Ordering::Release);
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        if let Some(inner) = &self.inner {
            render(inner);
            eprintln!();
        }
    }
}

fn render(inner: &ProgressInner) {
    let completed = inner.completed.load(Ordering::Relaxed).min(inner.total);
    let percent = completed.saturating_mul(100) / inner.total.max(1);
    let filled = percent / 5;
    let empty = 20usize.saturating_sub(filled);
    let elapsed = inner.started.elapsed().as_secs_f32();
    eprint!(
        "\r{:<24} [{}{}] {:>3}% {}/{} ops {:.1}s",
        inner.label,
        "#".repeat(filled),
        ".".repeat(empty),
        percent,
        completed,
        inner.total,
        elapsed
    );
    let _ = io::stderr().flush();
}
