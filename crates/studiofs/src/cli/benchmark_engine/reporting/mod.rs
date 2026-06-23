mod human;
mod json;

pub(crate) use human::{render_human_summary, render_public_bench_summary};
pub(crate) use json::save_benchmark_report;
