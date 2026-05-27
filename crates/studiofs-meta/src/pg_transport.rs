use crate::{MetaError, MetaResult};
use rustls::pki_types::ServerName;
use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect, TlsStream};
use tracing::info;

/// TLS stream type used by StudioFS PostgreSQL connections.
pub struct StudioPgTlsStream<S>(tokio_rustls::client::TlsStream<S>);

/// PostgreSQL connection type returned by the StudioFS transport helper.
pub type StudioPgConnection =
    tokio_postgres::Connection<tokio_postgres::Socket, StudioPgTlsStream<tokio_postgres::Socket>>;

#[derive(Clone)]
struct StudioPgTls {
    config: Arc<rustls::ClientConfig>,
}

struct StudioPgTlsConnect {
    hostname: String,
    connector: tokio_rustls::TlsConnector,
}

type StudioPgTlsFuture<S> =
    Pin<Box<dyn Future<Output = io::Result<StudioPgTlsStream<S>>> + Send + 'static>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgTargetClass {
    Local,
    Remote,
}

#[derive(Debug, Clone)]
struct PgTransportPolicy {
    target_class: PgTargetClass,
    display_host: String,
    sslmode: SslMode,
}

impl StudioPgTls {
    fn new() -> MetaResult<Self> {
        Ok(Self {
            config: shared_client_config()?,
        })
    }
}

fn shared_client_config() -> MetaResult<Arc<rustls::ClientConfig>> {
    static CLIENT_CONFIG: OnceLock<Result<Arc<rustls::ClientConfig>, String>> = OnceLock::new();

    CLIENT_CONFIG
        .get_or_init(|| {
            let root_store = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            let provider = rustls::crypto::ring::default_provider();
            let config = rustls::ClientConfig::builder_with_provider(provider.into())
                .with_safe_default_protocol_versions()
                .map_err(|error| format!("failed to configure postgres TLS versions: {error}"))?
                .with_root_certificates(root_store)
                .with_no_client_auth();

            Ok(Arc::new(config))
        })
        .as_ref()
        .map(Arc::clone)
        .map_err(|error| MetaError::Storage(error.clone()))
}

impl MakeTlsConnect<tokio_postgres::Socket> for StudioPgTls {
    type Stream = StudioPgTlsStream<tokio_postgres::Socket>;
    type TlsConnect = StudioPgTlsConnect;
    type Error = std::convert::Infallible;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        Ok(StudioPgTlsConnect {
            hostname: domain.to_string(),
            connector: Arc::clone(&self.config).into(),
        })
    }
}

impl<S> TlsConnect<S> for StudioPgTlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = StudioPgTlsStream<S>;
    type Error = io::Error;
    type Future = StudioPgTlsFuture<S>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let server_name = ServerName::try_from(self.hostname).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid postgres TLS server name: {error}"),
                )
            })?;
            self.connector
                .connect(server_name, stream)
                .await
                .map(StudioPgTlsStream)
        })
    }
}

impl<S> AsyncRead for StudioPgTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for StudioPgTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl<S> TlsStream for StudioPgTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

/// Open a PostgreSQL connection using the StudioFS transport policy.
pub async fn connect_studiofs_postgres(
    dsn: &str,
) -> MetaResult<(tokio_postgres::Client, StudioPgConnection)> {
    let config = parse_postgres_config(dsn)?;
    let policy = inspect_postgres_transport_policy(&config);
    log_postgres_transport_policy(&policy);
    config.connect(StudioPgTls::new()?).await.map_err(|error| {
        MetaError::Storage(format!(
            "postgres connect failed: {}",
            format_pg_error(error)
        ))
    })
}

fn parse_postgres_config(dsn: &str) -> MetaResult<tokio_postgres::Config> {
    tokio_postgres::Config::from_str(dsn)
        .map_err(|error| MetaError::Storage(format!("invalid postgres DSN: {error}")))
}

fn log_postgres_transport_policy(policy: &PgTransportPolicy) {
    if policy.target_class != PgTargetClass::Remote {
        return;
    }

    let key = format!("{:?}:{}", policy.sslmode, policy.display_host);
    let logged_policies = logged_transport_policies();
    if !logged_policies
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(key)
    {
        return;
    }

    match policy.sslmode {
        SslMode::Require => {}
        SslMode::Disable => {
            info!(
                host = %policy.display_host,
                "PostgreSQL TLS is disabled by DSN; using cleartext transport"
            );
        }
        SslMode::Prefer => {
            info!(
                host = %policy.display_host,
                "PostgreSQL TLS is preferred; falling back to cleartext if the server does not support TLS"
            );
        }
        _ => {}
    }
}

fn logged_transport_policies() -> &'static Mutex<HashSet<String>> {
    static LOGGED_POLICIES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    LOGGED_POLICIES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn inspect_postgres_transport_policy(config: &tokio_postgres::Config) -> PgTransportPolicy {
    let targets = classify_targets(config);
    let target_class = if targets
        .iter()
        .all(|target| target.0 == PgTargetClass::Local)
    {
        PgTargetClass::Local
    } else {
        PgTargetClass::Remote
    };
    let display_host = targets
        .iter()
        .find(|target| target.0 == PgTargetClass::Remote)
        .or_else(|| targets.first())
        .map(|target| target.1.clone())
        .unwrap_or_else(|| "implicit local PostgreSQL socket".to_string());

    PgTransportPolicy {
        target_class,
        display_host,
        sslmode: config.get_ssl_mode(),
    }
}

fn classify_targets(config: &tokio_postgres::Config) -> Vec<(PgTargetClass, String)> {
    let hostaddrs = config.get_hostaddrs();
    if !hostaddrs.is_empty() {
        let hosts = config.get_hosts();
        return hostaddrs
            .iter()
            .enumerate()
            .map(|(index, addr)| {
                (
                    classify_ip(*addr),
                    display_host_for_hostaddr(hosts, index).unwrap_or_else(|| addr.to_string()),
                )
            })
            .collect();
    }

    let hosts = config.get_hosts();
    if hosts.is_empty() {
        return vec![(
            PgTargetClass::Local,
            "implicit local PostgreSQL socket".to_string(),
        )];
    }

    hosts
        .iter()
        .map(|host| match host {
            Host::Tcp(host) => (classify_host(host), normalize_host_display(host)),
            #[cfg(unix)]
            Host::Unix(path) => (PgTargetClass::Local, path.display().to_string()),
        })
        .collect()
}

fn display_host_for_hostaddr(hosts: &[Host], index: usize) -> Option<String> {
    match hosts.get(index) {
        Some(Host::Tcp(host)) => Some(normalize_host_display(host)),
        #[cfg(unix)]
        Some(Host::Unix(path)) => Some(path.display().to_string()),
        None => None,
    }
}

fn classify_host(host: &str) -> PgTargetClass {
    let normalized = host.trim_matches(['[', ']']);
    if normalized.eq_ignore_ascii_case("localhost") || normalized.eq_ignore_ascii_case("localhost.")
    {
        return PgTargetClass::Local;
    }
    normalized
        .parse::<IpAddr>()
        .map_or(PgTargetClass::Remote, classify_ip)
}

fn classify_ip(addr: IpAddr) -> PgTargetClass {
    match addr {
        IpAddr::V4(addr) if addr.is_loopback() => PgTargetClass::Local,
        IpAddr::V6(addr) if addr.is_loopback() => PgTargetClass::Local,
        IpAddr::V6(addr) => addr.to_ipv4().map_or(PgTargetClass::Remote, |ipv4| {
            if ipv4.is_loopback() {
                PgTargetClass::Local
            } else {
                PgTargetClass::Remote
            }
        }),
        IpAddr::V4(_) => PgTargetClass::Remote,
    }
}

fn normalize_host_display(host: &str) -> String {
    host.trim_matches(['[', ']']).to_string()
}

fn format_pg_error(error: tokio_postgres::Error) -> String {
    if let Some(db) = error.as_db_error() {
        format!("{} (sqlstate={})", db.message(), db.code().code())
    } else {
        error.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PgTargetClass, PgTransportPolicy, classify_host, connect_studiofs_postgres,
        inspect_postgres_transport_policy, parse_postgres_config,
    };
    use crate::resolve_studiofs_pg_dsns;
    use std::sync::OnceLock;
    use tokio_postgres::config::SslMode;

    fn policy_for_dsn(dsn: &str) -> PgTransportPolicy {
        let config = parse_postgres_config(dsn).expect("parse postgres config");
        inspect_postgres_transport_policy(&config)
    }

    fn workspace_env_path() -> std::path::PathBuf {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(|path| path.parent())
            .map(std::path::Path::to_path_buf)
            .unwrap_or(manifest_dir)
            .join(".env")
    }

    fn load_test_env() {
        static ONCE: OnceLock<()> = OnceLock::new();
        let _ = ONCE.get_or_init(|| {
            let env_path = workspace_env_path();
            if env_path.is_file() {
                let _ = dotenvy::from_path(&env_path);
            } else {
                let _ = dotenvy::dotenv();
            }
        });
    }

    #[test]
    fn remote_url_without_sslmode_is_accepted_as_tls_preferred() {
        let policy = policy_for_dsn("postgres://user:secret@db.example.com/studiofs");

        assert_eq!(policy.target_class, PgTargetClass::Remote);
        assert_eq!(policy.sslmode, SslMode::Prefer);
    }

    #[test]
    fn remote_keyword_dsn_with_disable_is_accepted_as_explicit_cleartext() {
        let policy =
            policy_for_dsn("host=db.example.com user=studiofs password=secret sslmode=disable");

        assert_eq!(policy.target_class, PgTargetClass::Remote);
        assert_eq!(policy.sslmode, SslMode::Disable);
    }

    #[test]
    fn remote_dsn_with_require_is_accepted() {
        let policy =
            policy_for_dsn("postgres://user:secret@db.example.com/studiofs?sslmode=require");

        assert_eq!(policy.target_class, PgTargetClass::Remote);
        assert_eq!(policy.sslmode, SslMode::Require);
    }

    #[test]
    fn transport_policy_uses_driver_parsed_sslmode() {
        let config = parse_postgres_config(
            "host=db.example.com user=studiofs sslmode=prefer sslmode=require",
        )
        .expect("parse postgres config");
        let policy = inspect_postgres_transport_policy(&config);

        assert_eq!(policy.sslmode, SslMode::Require);
    }

    #[test]
    fn loopback_dsn_with_disable_is_accepted() {
        let ipv4_policy =
            policy_for_dsn("postgres://user:secret@127.42.0.7:5432/studiofs?sslmode=disable");
        assert_eq!(ipv4_policy.target_class, PgTargetClass::Local);
        assert_eq!(ipv4_policy.sslmode, SslMode::Disable);

        let ipv6_policy =
            policy_for_dsn("host=::1 port=5432 user=studiofs password=secret sslmode=disable");
        assert_eq!(ipv6_policy.target_class, PgTargetClass::Local);
        assert_eq!(ipv6_policy.sslmode, SslMode::Disable);
    }

    #[test]
    fn ipv4_mapped_loopback_dsn_with_disable_is_accepted() {
        let policy = policy_for_dsn(
            "postgres://user:secret@[::ffff:127.0.0.1]:5432/studiofs?sslmode=disable",
        );

        assert_eq!(policy.target_class, PgTargetClass::Local);
        assert_eq!(policy.sslmode, SslMode::Disable);
    }

    #[test]
    fn ipv4_compatible_loopback_host_is_local() {
        assert_eq!(classify_host("::127.0.0.1"), PgTargetClass::Local);
    }

    #[test]
    fn hostaddr_classifies_by_address_but_displays_host_name() {
        let policy =
            policy_for_dsn("host=db.example.com hostaddr=10.1.2.3 user=studiofs sslmode=disable");

        assert_eq!(policy.target_class, PgTargetClass::Remote);
        assert_eq!(policy.display_host, "db.example.com");
    }

    #[test]
    fn rewritten_pg_dsns_preserve_interpretable_transport_options() {
        let url_dsns = resolve_studiofs_pg_dsns(
            "postgres://user:secret@db.example.com/original?sslmode=require",
        )
        .expect("resolve url dsn");
        let keyword_dsns = resolve_studiofs_pg_dsns(
            "host=127.0.0.1 user=studiofs password=secret dbname=original sslmode=disable",
        )
        .expect("resolve keyword dsn");

        for dsn in [&url_dsns.admin_dsn, &url_dsns.metadata_dsn] {
            let policy = policy_for_dsn(dsn);
            assert_eq!(policy.target_class, PgTargetClass::Remote);
            assert_eq!(policy.sslmode, SslMode::Require);
        }
        for dsn in [&keyword_dsns.admin_dsn, &keyword_dsns.metadata_dsn] {
            let policy = policy_for_dsn(dsn);
            assert_eq!(policy.target_class, PgTargetClass::Local);
            assert_eq!(policy.sslmode, SslMode::Disable);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires TLS PostgreSQL backend (set STUDIOFS_TEST_PG_TLS_DSN or .env)"]
    async fn tls_postgres_fixture_connects_when_configured() {
        load_test_env();
        let Ok(dsn) = std::env::var("STUDIOFS_TEST_PG_TLS_DSN") else {
            eprintln!("skipping TLS PostgreSQL fixture: STUDIOFS_TEST_PG_TLS_DSN is not set");
            return;
        };
        if dsn.trim().is_empty() {
            eprintln!("skipping TLS PostgreSQL fixture: STUDIOFS_TEST_PG_TLS_DSN is empty");
            return;
        }

        let (client, connection) = connect_studiofs_postgres(&dsn)
            .await
            .expect("connect to TLS PostgreSQL fixture");
        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .simple_query("SELECT 1")
            .await
            .expect("TLS PostgreSQL fixture query");
        connection_task.abort();
    }
}
