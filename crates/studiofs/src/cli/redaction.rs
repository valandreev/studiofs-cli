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
            if url.query().is_some() {
                let redacted_pairs: Vec<(String, String)> = url
                    .query_pairs()
                    .map(|(key, value)| {
                        let value = if is_sensitive_postgres_key(&key) {
                            REDACTED.into()
                        } else {
                            value
                        };
                        (key.into_owned(), value.into_owned())
                    })
                    .collect();
                url.query_pairs_mut().clear().extend_pairs(redacted_pairs);
            }
            return url.to_string().replace("%3Credacted%3E", REDACTED);
        }
    }

    if let Some(redacted) = redact_url_like_postgres_dsn(dsn) {
        return redacted;
    }

    redact_keyword_postgres_dsn(dsn)
}

fn redact_url_like_postgres_dsn(dsn: &str) -> Option<String> {
    let scheme_end = dsn.find("://")?;
    let scheme = &dsn[..scheme_end];
    if !matches!(scheme, "postgres" | "postgresql") {
        return None;
    }

    let after_scheme = scheme_end + 3;
    let authority_end = dsn[after_scheme..]
        .find(['/', '?', '#'])
        .map_or(dsn.len(), |offset| after_scheme + offset);
    let authority = &dsn[after_scheme..authority_end];
    let rest = &dsn[authority_end..];

    let redacted_authority = if let Some(userinfo_end) = authority.rfind('@') {
        let userinfo = &authority[..userinfo_end];
        let hosts = &authority[userinfo_end + 1..];
        if userinfo.contains(':') {
            format!("{REDACTED}:{REDACTED}@{hosts}")
        } else if userinfo.is_empty() {
            authority.to_string()
        } else {
            format!("{REDACTED}@{hosts}")
        }
    } else {
        authority.to_string()
    };

    Some(format!(
        "{scheme}://{}{}",
        redacted_authority,
        redact_url_like_query(rest)
    ))
}

fn redact_url_like_query(rest: &str) -> String {
    let Some(query_start) = rest.find('?') else {
        return rest.to_string();
    };
    let fragment_start = rest[query_start + 1..]
        .find('#')
        .map(|offset| query_start + 1 + offset);
    let query_end = fragment_start.unwrap_or(rest.len());
    let mut redacted = String::from(&rest[..query_start + 1]);

    let mut first = true;
    for pair in rest[query_start + 1..query_end].split('&') {
        if !first {
            redacted.push('&');
        }
        first = false;
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if is_sensitive_postgres_key(key) {
            redacted.push_str(key);
            redacted.push('=');
            redacted.push_str(REDACTED);
        } else {
            redacted.push_str(pair);
            if !pair.contains('=') && !value.is_empty() {
                redacted.push('=');
                redacted.push_str(value);
            }
        }
    }
    redacted.push_str(&rest[query_end..]);
    redacted
}

fn redact_keyword_postgres_dsn(dsn: &str) -> String {
    let mut redacted = Vec::new();
    for part in split_keyword_dsn_parts(dsn) {
        let Some((key, _)) = part.split_once('=') else {
            redacted.push(part);
            continue;
        };

        let key = key.trim();
        if is_sensitive_postgres_key(key) {
            redacted.push(format!("{key}={REDACTED}"));
        } else {
            redacted.push(format!("{key}={}", part[key.len() + 1..].trim()));
        }
    }
    redacted.join(" ")
}

fn is_sensitive_postgres_key(key: &str) -> bool {
    matches!(key.to_ascii_lowercase().as_str(), "password" | "user")
}

fn split_keyword_dsn_parts(dsn: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut chars = dsn.char_indices().peekable();

    while let Some((_, ch)) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }

        let key_start = chars.peek().map_or(dsn.len(), |(idx, _)| *idx);
        while let Some((_, ch)) = chars.peek().copied() {
            if ch.is_whitespace() || ch == '=' {
                break;
            }
            chars.next();
        }

        let key_end = chars.peek().map_or(dsn.len(), |(idx, _)| *idx);
        while let Some((_, ch)) = chars.peek().copied() {
            if !ch.is_whitespace() {
                break;
            }
            chars.next();
        }

        if !matches!(chars.peek(), Some((_, '='))) {
            let end = chars.peek().map_or(dsn.len(), |(idx, _)| *idx);
            parts.push(dsn[key_start..end].to_string());
            continue;
        }
        chars.next();

        while let Some((_, ch)) = chars.peek().copied() {
            if !ch.is_whitespace() {
                break;
            }
            chars.next();
        }

        let value_start = chars.peek().map_or(dsn.len(), |(idx, _)| *idx);
        let mut value_end = value_start;
        let mut in_single_quote = false;
        let mut escaped = false;
        for (idx, ch) in chars.by_ref() {
            if escaped {
                value_end = idx + ch.len_utf8();
                escaped = false;
                continue;
            }
            if in_single_quote && ch == '\\' {
                value_end = idx + ch.len_utf8();
                escaped = true;
                continue;
            }
            if ch == '\'' {
                in_single_quote = !in_single_quote;
                value_end = idx + ch.len_utf8();
                continue;
            }
            if ch.is_whitespace() && !in_single_quote {
                break;
            }
            value_end = idx + ch.len_utf8();
        }

        parts.push(format!(
            "{}={}",
            dsn[key_start..key_end].trim(),
            dsn[value_start..value_end].trim()
        ));
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
    fn redacts_postgres_url_query_credentials() {
        let redacted = redact_postgres_dsn(
            "postgresql://db.example.test:5432/studiofs?user=lab_user&password=lab_password&sslmode=require",
        );

        assert_eq!(
            redacted,
            "postgresql://db.example.test:5432/studiofs?user=<redacted>&password=<redacted>&sslmode=require"
        );
        assert!(!redacted.contains("lab_user"));
        assert!(!redacted.contains("lab_password"));
    }

    #[test]
    fn redacts_multi_host_postgres_url_credentials() {
        let redacted = redact_postgres_dsn(
            "postgresql://lab_user:lab_password@host1.example.test:5432,host2.example.test:5433/studiofs?password=query_secret&target_session_attrs=read-write",
        );

        assert_eq!(
            redacted,
            "postgresql://<redacted>:<redacted>@host1.example.test:5432,host2.example.test:5433/studiofs?password=<redacted>&target_session_attrs=read-write"
        );
        assert!(!redacted.contains("lab_user"));
        assert!(!redacted.contains("lab_password"));
        assert!(!redacted.contains("query_secret"));
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
    fn redacts_keyword_postgres_values_with_spaces_around_equals() {
        let redacted = redact_postgres_dsn(
            "host = db.example.test user = lab_user password = lab_password dbname = studiofs",
        );

        assert_eq!(
            redacted,
            "host=db.example.test user=<redacted> password=<redacted> dbname=studiofs"
        );
        assert!(!redacted.contains("lab_user"));
        assert!(!redacted.contains("lab_password"));
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
}
