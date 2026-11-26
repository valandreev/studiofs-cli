/// Owns an empty PostgreSQL schema for a CLI test and its child processes.
pub(super) struct PgTestSchema {
    base_dsn: String,
    name: String,
    dsn: String,
}

impl PgTestSchema {
    pub(super) fn new(base_dsn: &str) -> anyhow::Result<Self> {
        let name = format!("sfs_cli_test_{}", uuid::Uuid::new_v4().simple());
        let dsn = schema_dsn(base_dsn, &name)?;
        execute(base_dsn, &format!("CREATE SCHEMA {name}"))?;
        Ok(Self {
            base_dsn: base_dsn.to_owned(),
            name,
            dsn,
        })
    }

    pub(super) fn dsn(&self) -> &str {
        &self.dsn
    }
}

impl Drop for PgTestSchema {
    fn drop(&mut self) {
        if let Err(error) = execute(
            &self.base_dsn,
            &format!("DROP SCHEMA {} CASCADE", self.name),
        ) {
            eprintln!(
                "failed to remove PostgreSQL test schema {}: {error}",
                self.name
            );
        }
    }
}

fn schema_dsn(base_dsn: &str, name: &str) -> anyhow::Result<String> {
    let options = format!("-csearch_path={name}");
    let dsn = base_dsn.trim();
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        let mut url =
            url::Url::parse(dsn).map_err(|_| anyhow::anyhow!("invalid PostgreSQL test URL"))?;
        url.query_pairs_mut().append_pair("options", &options);
        Ok(url.into())
    } else {
        Ok(format!("{dsn} options='{options}'"))
    }
}

fn execute(dsn: &str, sql: &str) -> anyhow::Result<()> {
    crate::runtime_bridge::block_on(super::init::with_postgres_client(
        dsn,
        |client| async move {
            client.batch_execute("SET lock_timeout = '5s'").await?;
            client.batch_execute(sql).await?;
            Ok(())
        },
    ))
}

#[test]
fn schema_dsn_replaces_existing_search_path() {
    for dsn in [
        "postgresql://localhost/studiofs?options=-csearch_path%3Dpublic",
        "host=localhost dbname=studiofs options='-csearch_path=public'",
    ] {
        let config: tokio_postgres::Config = schema_dsn(dsn, "sfs_cli_test_example")
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(config.get_dbname(), Some("studiofs"));
        assert_eq!(
            config.get_options(),
            Some("-csearch_path=sfs_cli_test_example")
        );
    }
}
