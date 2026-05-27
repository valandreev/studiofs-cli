use crate::{MetaError, MetaResult, connect_studiofs_postgres};
use std::str::FromStr;

/// Canonical PostgreSQL database name used for StudioFS metadata.
pub const STUDIOFS_METADATA_DB: &str = "studiofs";
const PG_ADMIN_DB: &str = "postgres";

#[derive(Debug, Clone)]
/// Pair of DSNs used to provision and then connect to the StudioFS metadata DB.
pub struct StudioPgDsns {
    /// DSN that targets an administrative database such as `postgres`.
    pub admin_dsn: String,
    /// DSN that targets the `studiofs` metadata database itself.
    pub metadata_dsn: String,
}

/// Resolve an input PostgreSQL DSN into admin and StudioFS metadata DSNs.
pub fn resolve_studiofs_pg_dsns(input: &str) -> MetaResult<StudioPgDsns> {
    let input = input.trim();
    if input.is_empty() {
        return Err(MetaError::Storage(
            "metadata DSN must not be empty".to_string(),
        ));
    }

    let (admin_dsn, metadata_dsn) = if input.contains("://") {
        (
            rewrite_url_dsn(input, PG_ADMIN_DB)?,
            rewrite_url_dsn(input, STUDIOFS_METADATA_DB)?,
        )
    } else {
        (
            rewrite_keyword_dsn(input, PG_ADMIN_DB)?,
            rewrite_keyword_dsn(input, STUDIOFS_METADATA_DB)?,
        )
    };

    Ok(StudioPgDsns {
        admin_dsn,
        metadata_dsn,
    })
}

/// Ensure the `studiofs` PostgreSQL database exists and return both DSNs.
pub async fn ensure_studiofs_database(input: &str) -> MetaResult<StudioPgDsns> {
    let dsns = resolve_studiofs_pg_dsns(input)?;
    let (client, connection) = connect_studiofs_postgres(&dsns.admin_dsn).await?;
    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            &[&STUDIOFS_METADATA_DB],
        )
        .await
        .map_err(|e| MetaError::Storage(format!("postgres database probe failed: {e}")))?
        .is_some();

    if !exists {
        client
            .execute(&format!("CREATE DATABASE {STUDIOFS_METADATA_DB}"), &[])
            .await
            .map_err(|e| MetaError::Storage(format!("postgres create database failed: {e}")))?;
    }

    conn_task.abort();
    if let Err(err) = conn_task.await
        && !err.is_cancelled()
    {
        return Err(MetaError::Storage(format!(
            "postgres connection task failed: {err}"
        )));
    }
    Ok(dsns)
}

fn rewrite_url_dsn(input: &str, db_name: &str) -> MetaResult<String> {
    let mut url = url::Url::parse(input)
        .map_err(|e| MetaError::Storage(format!("invalid postgres URL: {e}")))?;
    match url.scheme() {
        "postgres" | "postgresql" => {}
        other => {
            return Err(MetaError::Storage(format!(
                "unsupported metadata URL scheme '{other}'"
            )));
        }
    }
    url.set_path(&format!("/{db_name}"));
    Ok(url.to_string())
}

fn rewrite_keyword_dsn(input: &str, db_name: &str) -> MetaResult<String> {
    tokio_postgres::Config::from_str(input)
        .map_err(|e| MetaError::Storage(format!("invalid postgres DSN: {e}")))?;

    let mut tokens = split_keyword_dsn(input);
    let mut replaced = false;

    for token in &mut tokens {
        if let Some((key, _)) = token.split_once('=')
            && key.eq_ignore_ascii_case("dbname")
        {
            *token = format!("dbname={db_name}");
            replaced = true;
        }
    }

    if !replaced {
        tokens.push(format!("dbname={db_name}"));
    }

    Ok(tokens.join(" "))
}

fn split_keyword_dsn(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;

    for ch in input.chars() {
        match quote {
            Some(q) if ch == q => {
                quote = None;
                current.push(ch);
            }
            Some(_) => current.push(ch),
            None if ch == '\'' || ch == '"' => {
                quote = Some(ch);
                current.push(ch);
            }
            None if ch.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            None => current.push(ch),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_url_dsn() {
        let dsns =
            resolve_studiofs_pg_dsns("postgres://u:p@127.0.0.1:5432").expect("resolve url dsn");
        assert_eq!(dsns.admin_dsn, "postgres://u:p@127.0.0.1:5432/postgres");
        assert_eq!(dsns.metadata_dsn, "postgres://u:p@127.0.0.1:5432/studiofs");
    }

    #[test]
    fn test_resolve_keyword_dsn() {
        let dsns = resolve_studiofs_pg_dsns(
            "host=127.0.0.1 port=5432 user=studiofs password=studiofs sslmode=disable",
        )
        .expect("resolve keyword dsn");
        assert!(dsns.admin_dsn.contains("dbname=postgres"));
        assert!(dsns.metadata_dsn.contains("dbname=studiofs"));
        assert!(dsns.admin_dsn.contains("sslmode=disable"));
        assert!(dsns.metadata_dsn.contains("sslmode=disable"));
    }

    #[test]
    fn test_resolve_url_dsn_preserves_transport_query_options() {
        let dsns = resolve_studiofs_pg_dsns(
            "postgres://u:p@db.example.com:5432/original?sslmode=require&application_name=studiofs",
        )
        .expect("resolve url dsn");
        assert_eq!(
            dsns.admin_dsn,
            "postgres://u:p@db.example.com:5432/postgres?sslmode=require&application_name=studiofs"
        );
        assert_eq!(
            dsns.metadata_dsn,
            "postgres://u:p@db.example.com:5432/studiofs?sslmode=require&application_name=studiofs"
        );
    }

    #[test]
    fn test_resolve_keyword_dsn_preserves_tls_required_transport_options() {
        let dsns = resolve_studiofs_pg_dsns(
            "host=db.example.com port=5432 user=studiofs password=secret sslmode=require application_name=studiofs",
        )
        .expect("resolve keyword dsn");

        assert!(dsns.admin_dsn.contains("dbname=postgres"));
        assert!(dsns.metadata_dsn.contains("dbname=studiofs"));
        assert!(dsns.admin_dsn.contains("sslmode=require"));
        assert!(dsns.metadata_dsn.contains("sslmode=require"));
        assert!(dsns.admin_dsn.contains("application_name=studiofs"));
        assert!(dsns.metadata_dsn.contains("application_name=studiofs"));
    }

    #[test]
    fn test_override_existing_dbname() {
        let dsns = resolve_studiofs_pg_dsns(
            "host=127.0.0.1 port=5432 user=studiofs password=studiofs dbname=otherdb",
        )
        .expect("resolve keyword dsn");
        assert!(!dsns.metadata_dsn.contains("dbname=otherdb"));
        assert!(dsns.metadata_dsn.contains("dbname=studiofs"));
    }
}
