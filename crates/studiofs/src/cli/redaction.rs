//! Redaction helpers for user-facing CLI and diagnostic output.

use url::Url;

const REDACTED: &str = "<redacted>";

pub(crate) fn redact_postgres_dsn(dsn: &str) -> String {
    if let Ok(mut url) = Url::parse(dsn) {
        if matches!(url.scheme(), "postgres" | "postgresql") {
            if !url.username().is_empty() {
                let _ = url.set_username(REDACTED);
            }
            if url.password().is_some() {
                let _ = url.set_password(Some(REDACTED));
            }
            return url.to_string().replace("%3Credacted%3E", REDACTED);
        }
    }

    redact_keyword_postgres_dsn(dsn)
}

#[allow(dead_code)]
pub(crate) fn redact_secret(value: &str) -> &str {
    if value.is_empty() { "" } else { REDACTED }
}

fn redact_keyword_postgres_dsn(dsn: &str) -> String {
    let mut redacted = Vec::new();
    for part in split_keyword_dsn_parts(dsn) {
        let Some((key, _)) = part.split_once('=') else {
            redacted.push(part);
            continue;
        };

        if matches!(key.to_ascii_lowercase().as_str(), "password" | "user") {
            redacted.push(format!("{key}={REDACTED}"));
        } else {
            redacted.push(part);
        }
    }
    redacted.join(" ")
}

fn split_keyword_dsn_parts(dsn: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut escaped = false;

    for ch in dsn.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if in_single_quote && ch == '\\' {
            current.push(ch);
            escaped = true;
            continue;
        }
        if ch == '\'' {
            in_single_quote = !in_single_quote;
            current.push(ch);
            continue;
        }
        if ch.is_whitespace() && !in_single_quote {
            if !current.is_empty() {
                parts.push(std::mem::take(&mut current));
            }
            continue;
        }
        current.push(ch);
    }

    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_postgres_url_credentials_but_keeps_backend_context() {
        let redacted =
            redact_postgres_dsn("postgres://lab_user:lab_password@db.example.test:5432/studiofs");

        assert_eq!(
            redacted,
            "postgres://<redacted>:<redacted>@db.example.test:5432/studiofs"
        );
        assert!(!redacted.contains("lab_user"));
        assert!(!redacted.contains("lab_password"));
    }

    #[test]
    fn redacts_keyword_postgres_user_and_password() {
        let redacted = redact_postgres_dsn(
            "host=db.example.test port=5432 user=lab_user password=lab_password dbname=studiofs",
        );

        assert_eq!(
            redacted,
            "host=db.example.test port=5432 user=<redacted> password=<redacted> dbname=studiofs"
        );
    }

    #[test]
    fn redacts_keyword_postgres_values_without_splitting_quoted_spaces() {
        let redacted = redact_postgres_dsn(
            "host=db.example.test user='lab user' password='lab password' dbname='my db'",
        );

        assert_eq!(
            redacted,
            "host=db.example.test user=<redacted> password=<redacted> dbname='my db'"
        );
        assert!(!redacted.contains("lab user"));
        assert!(!redacted.contains("lab password"));
    }

    #[test]
    fn redacts_empty_and_partial_keyword_postgres_dsn_without_panicking() {
        assert_eq!(redact_postgres_dsn(""), "");
        assert_eq!(
            redact_postgres_dsn("host=db.example.test"),
            "host=db.example.test"
        );
        assert_eq!(redact_postgres_dsn("password="), "password=<redacted>");
    }

    #[test]
    fn redacts_non_empty_s3_keys_for_display() {
        assert_eq!(redact_secret("AKIA_TEST"), "<redacted>");
        assert_eq!(redact_secret("secret-value"), "<redacted>");
        assert_eq!(redact_secret(""), "");
    }
}
