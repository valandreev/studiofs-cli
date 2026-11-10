//! Live S3 smoke test for the janitor's batched delete path.
//!
//! Run explicitly with:
//! `cargo test -p studiofs-data --test janitor_live -- --ignored --nocapture`

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::TryStreamExt as _;
use opendal::layers::HttpClientLayer;
use opendal::raw::{HttpBody, HttpClient, HttpFetch};
use opendal::services::S3;
use opendal::{Buffer, Operator, Result};
use studiofs_data::janitor::JanitorObjectStore;
use uuid::Uuid;

type TestResult<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Default)]
struct Requests {
    batch_delete_posts: AtomicUsize,
    scalar_deletes: AtomicUsize,
}

struct CountingHttpClient {
    inner: HttpClient,
    requests: Arc<Requests>,
}

impl HttpFetch for CountingHttpClient {
    async fn fetch(&self, request: http::Request<Buffer>) -> Result<http::Response<HttpBody>> {
        if request.method() == http::Method::POST
            && request.uri().query().is_some_and(|query| {
                query
                    .split('&')
                    .any(|part| part == "delete" || part == "delete=")
            })
        {
            self.requests
                .batch_delete_posts
                .fetch_add(1, Ordering::Relaxed);
        } else if request.method() == http::Method::DELETE {
            self.requests.scalar_deletes.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.fetch(request).await
    }
}

fn workspace_env_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .unwrap()
        .join(".env")
}

fn test_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            dotenvy::from_path_iter(workspace_env_path())
                .ok()?
                .filter_map(std::result::Result::ok)
                .find_map(|(name, value)| (name == key).then_some(value))
                .filter(|value| !value.trim().is_empty())
        })
}

fn require_env(key: &str) -> TestResult<String> {
    test_env(key).ok_or_else(|| format!("missing required {key} setting").into())
}

fn b2_region(endpoint: &str) -> TestResult<String> {
    let host = url::Url::parse(endpoint)?;
    host.host_str()
        .and_then(|host| host.split('.').nth(1))
        .filter(|region| !region.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| "STUDIOFS_S3_REGION is required when endpoint has no region host".into())
}

async fn versions(operator: &Operator, prefix: &str) -> TestResult<Vec<(String, String)>> {
    let mut listing = operator
        .lister_with(prefix)
        .recursive(true)
        .versions(true)
        .deleted(true)
        .await
        .map_err(|error| format!("version listing failed: {:?}", error.kind()))?;
    let mut versions = Vec::new();
    while let Some(entry) = listing
        .try_next()
        .await
        .map_err(|error| format!("version listing failed: {:?}", error.kind()))?
    {
        let version = entry
            .metadata()
            .version()
            .ok_or_else(|| "version listing omitted a version ID".to_owned())?;
        versions.push((entry.path().to_owned(), version.to_owned()));
    }
    Ok(versions)
}

async fn cleanup(operator: &Operator, prefix: &str, keys: &[String]) -> TestResult<usize> {
    let entries = versions(operator, prefix).await?;
    let count = entries.len();
    for (key, version) in entries {
        if keys.contains(&key) {
            operator
                .delete_with(&key)
                .version(&version)
                .await
                .map_err(|error| format!("scratch version cleanup failed: {:?}", error.kind()))?;
        }
    }
    let leftovers = versions(operator, prefix).await?;
    if !leftovers.is_empty() {
        return Err("scratch versions or delete markers remain after cleanup".into());
    }
    Ok(count)
}

async fn smoke(
    operator: &Operator,
    store: &JanitorObjectStore,
    prefix: &str,
    keys: &[String],
    protected: &str,
    requests: &Requests,
) -> TestResult<(usize, usize)> {
    for key in keys
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(protected))
    {
        operator
            .write(key, b"sfs1065 live janitor smoke".to_vec())
            .await
            .map_err(|error| format!("scratch seed failed: {:?}", error.kind()))?;
    }

    let batches_before = requests.batch_delete_posts.load(Ordering::Relaxed);
    let deletes_before = requests.scalar_deletes.load(Ordering::Relaxed);
    let results = store.delete_batch(keys).await;
    if results.len() != keys.len() || results.iter().any(std::result::Result::is_err) {
        return Err("janitor batch delete did not confirm every key".into());
    }
    let batch_posts = requests
        .batch_delete_posts
        .load(Ordering::Relaxed)
        .saturating_sub(batches_before);
    let scalar_deletes = requests
        .scalar_deletes
        .load(Ordering::Relaxed)
        .saturating_sub(deletes_before);
    if batch_posts != 1 || scalar_deletes != 0 {
        return Err("janitor delete did not use exactly one multi-delete request".into());
    }

    let visible = operator
        .list(prefix)
        .await
        .map_err(|error| format!("scratch verification failed: {:?}", error.kind()))?;
    if keys
        .iter()
        .any(|key| visible.iter().any(|entry| entry.path() == key))
    {
        return Err("a deleted scratch key remains visible".into());
    }
    let payload = operator
        .read(protected)
        .await
        .map_err(|error| format!("protected sentinel read failed: {:?}", error.kind()))?;
    if payload.to_vec() != b"sfs1065 live janitor smoke" {
        return Err("protected sentinel content changed".into());
    }
    Ok((batch_posts, scalar_deletes))
}

#[tokio::test]
#[ignore = "requires explicitly configured live S3 credentials and an empty scratch prefix"]
async fn janitor_live_s3_uses_multi_delete_and_cleans_exact_versions() -> TestResult<()> {
    let endpoint = require_env("STUDIOFS_S3_ENDPOINT")?;
    let bucket = require_env("STUDIOFS_S3_BUCKET")?;
    let access_key = require_env("STUDIOFS_S3_ACCESS_KEY")?;
    let secret_key = require_env("STUDIOFS_S3_SECRET_KEY")?;
    let region = test_env("STUDIOFS_S3_REGION").map_or_else(|| b2_region(&endpoint), Ok)?;
    let prefix = format!("sfs1065-smoke/{}/", Uuid::new_v4());
    let keys: Vec<_> = (0..4)
        .map(|index| format!("{prefix}seed-{index}"))
        .collect();
    let protected = format!("{prefix}protected-sentinel");
    let requests = Arc::new(Requests::default());
    let operator = Operator::new(
        S3::default()
            .endpoint(&endpoint)
            .bucket(&bucket)
            .access_key_id(&access_key)
            .secret_access_key(&secret_key)
            .region(&region)
            .disable_config_load()
            .disable_ec2_metadata()
            .enable_versioning(true),
    )?
    .layer(HttpClientLayer::new(HttpClient::with(CountingHttpClient {
        inner: HttpClient::new()?,
        requests: requests.clone(),
    })))
    .finish();
    let store = JanitorObjectStore::new(operator.clone());

    if !versions(&operator, &prefix).await?.is_empty() {
        return Err("scratch prefix unexpectedly contains versions".into());
    }
    let artifact_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .unwrap()
        .join("target/janitor-benchmark");
    std::fs::create_dir_all(&artifact_dir)?;
    let artifact = artifact_dir.join("sfs1065-live-smoke.json");
    // Retain the exact scratch scope for recovery even if the provider rejects cleanup.
    std::fs::write(
        &artifact,
        format!("{{\"prefix\":\"{prefix}\",\"status\":\"started\"}}\n"),
    )?;
    let result = smoke(&operator, &store, &prefix, &keys, &protected, &requests).await;
    let cleanup_result = cleanup(
        &operator,
        &prefix,
        &keys.iter().cloned().chain([protected]).collect::<Vec<_>>(),
    )
    .await;
    let versions_removed = cleanup_result?;
    let (batch_posts, scalar_deletes) = result?;
    std::fs::write(
        artifact,
        format!(
            "{{\"prefix\":\"{prefix}\",\"batch_delete_posts\":{batch_posts},\"scalar_delete_requests\":{scalar_deletes},\"deleted_keys\":4,\"protected_sentinel_intact\":true,\"versions_removed\":{versions_removed},\"versions_remaining\":0}}\n"
        ),
    )?;
    Ok(())
}
