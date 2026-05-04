use std::collections::{BTreeMap, BTreeSet};

pub fn seed_order_signatures(seed_orders: &[(u64, Vec<String>)]) -> BTreeSet<String> {
    let mut by_seed = BTreeMap::<u64, Vec<String>>::new();
    for (seed, orders) in seed_orders {
        by_seed.entry(*seed).or_default().extend(orders.clone());
    }
    by_seed
        .into_values()
        .map(|orders| orders.join(">"))
        .collect()
}
