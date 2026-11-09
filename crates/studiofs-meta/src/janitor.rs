//! Metadata protection snapshot used by standalone object cleanup.

use std::collections::{HashMap, HashSet};

use crate::{MetaEngine, MetaResult};

/// Metadata facts that protect objects from standalone janitor deletion.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct JanitorProtection {
    /// Objects reachable from the current metadata namespace.
    pub referenced: HashSet<u64>,
    /// Objects retained by global snapshot or lifecycle pins.
    pub snapshot_pins: HashSet<u64>,
    /// Objects with pending deletion timestamps.
    pub pending_deletions: HashMap<u64, i64>,
}

/// Minimal metadata operations needed to build a janitor protection snapshot.
pub trait JanitorProtectionSource {
    /// Return global snapshot pins.
    fn snapshot_pins(&self) -> MetaResult<HashSet<u64>>;
    /// Retire pending recovery roots before destructive cleanup.
    fn reconcile_recoveries(&self) -> MetaResult<()>;
    /// Return currently referenced objects.
    fn referenced_objects(&self) -> MetaResult<HashSet<u64>>;
    /// Return pending deletion timestamps.
    fn pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>>;
}

impl<T: MetaEngine + ?Sized> JanitorProtectionSource for T {
    fn snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
        self.get_global_snapshot_pins()
    }

    fn reconcile_recoveries(&self) -> MetaResult<()> {
        self.reconcile_pending_regular_file_recoveries()
    }

    fn referenced_objects(&self) -> MetaResult<HashSet<u64>> {
        self.get_referenced_object_ids()
    }

    fn pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
        self.get_pending_deletions()
    }
}

/// Read metadata protection facts in the established standalone-janitor order.
pub fn collect_janitor_protection(
    source: &(impl JanitorProtectionSource + ?Sized),
    dry_run: bool,
) -> MetaResult<JanitorProtection> {
    let snapshot_pins = source.snapshot_pins()?;
    if !dry_run {
        source.reconcile_recoveries()?;
    }
    let referenced = source.referenced_objects()?;
    let pending_deletions = source
        .pending_deletions()?
        .into_iter()
        .collect::<HashMap<_, _>>();
    Ok(JanitorProtection {
        referenced,
        snapshot_pins,
        pending_deletions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaError;
    use std::cell::RefCell;

    struct FakeProtectionSource {
        calls: RefCell<Vec<&'static str>>,
        fail_at: Option<&'static str>,
    }

    impl FakeProtectionSource {
        fn step(&self, name: &'static str) -> MetaResult<()> {
            self.calls.borrow_mut().push(name);
            if self.fail_at == Some(name) {
                return Err(MetaError::Storage(format!("{name} failed")));
            }
            Ok(())
        }
    }

    impl JanitorProtectionSource for FakeProtectionSource {
        fn snapshot_pins(&self) -> MetaResult<HashSet<u64>> {
            self.step("pins")?;
            Ok(HashSet::from([2]))
        }

        fn reconcile_recoveries(&self) -> MetaResult<()> {
            self.step("reconcile")
        }

        fn referenced_objects(&self) -> MetaResult<HashSet<u64>> {
            self.step("references")?;
            Ok(HashSet::from([1]))
        }

        fn pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>> {
            self.step("pending")?;
            Ok(vec![(3, 42)])
        }
    }

    #[test]
    fn protection_snapshot_preserves_dry_run_and_delete_order() {
        for (dry_run, expected) in [
            (true, vec!["pins", "references", "pending"]),
            (false, vec!["pins", "reconcile", "references", "pending"]),
        ] {
            let source = FakeProtectionSource {
                calls: RefCell::new(Vec::new()),
                fail_at: None,
            };
            let snapshot = collect_janitor_protection(&source, dry_run).unwrap();
            assert_eq!(*source.calls.borrow(), expected);
            assert_eq!(snapshot.referenced, HashSet::from([1]));
            assert_eq!(snapshot.snapshot_pins, HashSet::from([2]));
            assert_eq!(snapshot.pending_deletions, HashMap::from([(3, 42)]));
        }
    }

    #[test]
    fn protection_failure_stops_before_later_metadata_reads() {
        let source = FakeProtectionSource {
            calls: RefCell::new(Vec::new()),
            fail_at: Some("references"),
        };
        assert!(collect_janitor_protection(&source, false).is_err());
        assert_eq!(
            *source.calls.borrow(),
            vec!["pins", "reconcile", "references"]
        );
    }
}
