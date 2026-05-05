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
    for part in dsn.split_whitespace() {
        let Some((key, _)) = part.split_once('=') else {
            redacted.push(part.to_string());
            continue;
        };

        if matches!(key.to_ascii_lowercase().as_str(), "password" | "user") {
            redacted.push(format!("{key}={REDACTED}"));
        } else {
            redacted.push(part.to_string());
        }
    }
    redacted.join(" ")
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
    fn redacts_non_empty_s3_keys_for_display() {
        assert_eq!(redact_secret("AKIA_TEST"), "<redacted>");
        assert_eq!(redact_secret("secret-value"), "<redacted>");
        assert_eq!(redact_secret(""), "");
    }
}
