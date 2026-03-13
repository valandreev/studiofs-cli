//! Internal S3 SigV4 helpers used by CLI init flows.

use anyhow::{Context, anyhow};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

fn hmac_sha256_raw(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can accept any key length");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex_str(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Format current timestamp as AWS SigV4 datetime (`YYYYMMDDTHHmmssZ`) and date (`YYYYMMDD`).
fn fmt_datetime_now() -> (String, String) {
    use chrono::{DateTime, Utc};
    let dt: DateTime<Utc> = Utc::now();
    let datetime = dt.format("%Y%m%dT%H%M%SZ").to_string();
    let date = dt.format("%Y%m%d").to_string();
    (datetime, date)
}

#[allow(clippy::too_many_arguments)]
fn build_signed_headers(
    method: &str,
    endpoint_url: &str,
    path: &str,
    query: &str,
    body: &[u8],
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let url = reqwest::Url::parse(endpoint_url).context("invalid endpoint URL")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("endpoint URL has no host"))?;
    let host_with_port = match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    };

    let (datetime, date) = fmt_datetime_now();
    let payload_hash = sha256_hex_str(body);
    let content_len = body.len().to_string();

    let content_type = if body.is_empty() {
        "".to_string()
    } else {
        "application/xml".to_string()
    };

    let mut canon_hdrs: Vec<(String, String)> = vec![
        ("host".to_string(), host_with_port.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), datetime.clone()),
    ];
    if !content_type.is_empty() {
        canon_hdrs.push(("content-type".to_string(), content_type.clone()));
    }
    canon_hdrs.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers_str: String = canon_hdrs
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let signed_headers_str: String = canon_hdrs
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{path}\n{query}\n{canonical_headers_str}\n{signed_headers_str}\n{payload_hash}"
    );
    let cr_hash = sha256_hex_str(canonical_request.as_bytes());

    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{cr_hash}");

    let k_date = hmac_sha256_raw(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256_raw(&k_date, region.as_bytes());
    let k_service = hmac_sha256_raw(&k_region, b"s3");
    let k_signing = hmac_sha256_raw(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256_raw(&k_signing, string_to_sign.as_bytes()));

    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={signed_headers_str},Signature={signature}"
    );

    let mut out = vec![
        ("Host".to_string(), host_with_port),
        ("X-Amz-Date".to_string(), datetime),
        ("X-Amz-Content-Sha256".to_string(), payload_hash),
        ("Authorization".to_string(), auth),
        ("Content-Length".to_string(), content_len),
    ];
    if !content_type.is_empty() {
        out.push(("Content-Type".to_string(), content_type));
    }
    Ok(out)
}

pub(crate) async fn bucket_create(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<()> {
    let path = format!("/{bucket}");

    // For us-east-1 the body must be empty; other regions require LocationConstraint XML.
    let body: Vec<u8> = if region == "us-east-1" {
        vec![]
    } else {
        format!(
            "<CreateBucketConfiguration><LocationConstraint>{region}</LocationConstraint></CreateBucketConfiguration>"
        )
        .into_bytes()
    };

    let headers = build_signed_headers(
        "PUT", endpoint, &path, "", &body, access_key, secret_key, region,
    )?;

    let url = format!("{endpoint}{path}");
    let client = reqwest::Client::new();
    let mut rb = client.put(&url).body(body);
    for (k, v) in &headers {
        rb = rb.header(k.as_str(), v.as_str());
    }

    let resp = rb.send().await.context("HTTP request failed")?;
    let status = resp.status();
    match status.as_u16() {
        200 | 204 => Ok(()),
        // BucketAlreadyOwnedByYou / BucketAlreadyExists.
        409 => Ok(()),
        _ => {
            let body_text = resp.text().await.unwrap_or_default();
            Err(anyhow!("HTTP {status}: {body_text}"))
        }
    }
}
