use crate::{MetaError, MetaResult};
use rustls::pki_types::ServerName;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
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
    sslmode: Option<String>,
}

impl StudioPgTls {
    fn new() -> MetaResult<Self> {
        let root_store = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let provider = rustls::crypto::ring::default_provider();
        let config = rustls::ClientConfig::builder_with_provider(provider.into())
            .with_safe_default_protocol_versions()
            .map_err(|error| {
                MetaError::Storage(format!(
                    "failed to configure postgres TLS versions: {error}"
                ))
            })?
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(Self {
            config: Arc::new(config),
        })
    }
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

/// Validate the StudioFS PostgreSQL transport policy without opening a socket.
pub fn validate_postgres_transport_policy(dsn: &str) -> MetaResult<()> {
    inspect_postgres_transport_policy(dsn)?;
    Ok(())
}

/// Open a PostgreSQL connection using the StudioFS transport policy.
pub async fn connect_studiofs_postgres(
    dsn: &str,
) -> MetaResult<(tokio_postgres::Client, StudioPgConnection)> {
    let policy = inspect_postgres_transport_policy(dsn)?;
    log_postgres_transport_policy(&policy);
    let config = tokio_postgres::Config::from_str(dsn)
        .map_err(|error| MetaError::Storage(format!("invalid postgres DSN: {error}")))?;
    config.connect(StudioPgTls::new()?).await.map_err(|error| {
        MetaError::Storage(format!(
            "postgres connect failed: {}",
            format_pg_error(error)
        ))
    })
}

fn log_postgres_transport_policy(policy: &PgTransportPolicy) {
    if policy.target_class != PgTargetClass::Remote {
        return;
    }

    match policy
        .sslmode
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("require") => {}
        Some("disable") => {
            info!(
                host = %policy.display_host,
                "PostgreSQL TLS is disabled by DSN; using cleartext transport"
            );
        }
        _ => {
            info!(
                host = %policy.display_host,
                "PostgreSQL TLS is preferred; falling back to cleartext if the server does not support TLS"
            );
        }
    }
}

fn inspect_postgres_transport_policy(dsn: &str) -> MetaResult<PgTransportPolicy> {
    let config = tokio_postgres::Config::from_str(dsn)
        .map_err(|error| MetaError::Storage(format!("invalid postgres DSN: {error}")))?;
    let sslmode = explicit_sslmode(dsn);
    let targets = classify_targets(&config);
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

    Ok(PgTransportPolicy {
        target_class,
        display_host,
        sslmode: sslmode.or_else(|| match config.get_ssl_mode() {
            SslMode::Disable => Some("disable".to_string()),
            SslMode::Require => Some("require".to_string()),
            SslMode::Prefer => None,
            _ => None,
        }),
    })
}

fn classify_targets(config: &tokio_postgres::Config) -> Vec<(PgTargetClass, String)> {
    let hostaddrs = config.get_hostaddrs();
    if !hostaddrs.is_empty() {
        return hostaddrs
            .iter()
            .map(|addr| {
                (
                    classify_ip(*addr),
                    normalize_host_display(&addr.to_string()),
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
        IpAddr::V6(addr) => embedded_ipv4(addr).map_or(PgTargetClass::Remote, |ipv4| {
            if ipv4.is_loopback() {
                PgTargetClass::Local
            } else {
                PgTargetClass::Remote
            }
        }),
        IpAddr::V4(_) => PgTargetClass::Remote,
    }
}

fn embedded_ipv4(addr: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(ipv4) = addr.to_ipv4_mapped() {
        return Some(ipv4);
    }

    let segments = addr.segments();
    if segments[..6] == [0, 0, 0, 0, 0, 0] && (segments[6] != 0 || segments[7] != 0) {
        let octets = [
            (segments[6] >> 8) as u8,
            segments[6] as u8,
            (segments[7] >> 8) as u8,
            segments[7] as u8,
        ];
        return Some(Ipv4Addr::from(octets));
    }

    None
}

fn explicit_sslmode(dsn: &str) -> Option<String> {
    if dsn.contains("://") {
        return url::Url::parse(dsn).ok().and_then(|url| {
            url.query_pairs()
                .find(|(key, _)| key.eq_ignore_ascii_case("sslmode"))
                .map(|(_, value)| value.into_owned())
        });
    }

    split_keyword_dsn(dsn).into_iter().find_map(|token| {
        let (key, value) = token.split_once('=')?;
        if key.eq_ignore_ascii_case("sslmode") {
            Some(unquote_keyword_value(value).to_string())
        } else {
            None
        }
    })
}

fn normalize_host_display(host: &str) -> String {
    host.trim_matches(['[', ']']).to_string()
}

fn split_keyword_dsn(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match quote {
            Some(_) if ch == '\\' => {
                current.push(ch);
                escaped = true;
            }
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

fn unquote_keyword_value(value: &str) -> &str {
    value
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .or_else(|| {
            value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
        })
        .unwrap_or(value)
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
    use super::validate_postgres_transport_policy;

    #[test]
    fn remote_url_without_sslmode_is_accepted_as_tls_preferred() {
        validate_postgres_transport_policy("postgres://user:secret@db.example.com/studiofs")
            .expect("remote DSN without sslmode should use TLS-preferred behavior");
    }

    #[test]
    fn remote_keyword_dsn_with_disable_is_accepted_as_explicit_cleartext() {
        validate_postgres_transport_policy(
            "host=db.example.com user=studiofs password=secret sslmode=disable",
        )
        .expect("explicit cleartext DSN should be accepted with informational logging");
    }

    #[test]
    fn remote_dsn_with_require_is_accepted() {
        validate_postgres_transport_policy(
            "postgres://user:secret@db.example.com/studiofs?sslmode=require",
        )
        .expect("remote sslmode=require should be accepted");
    }

    #[test]
    fn loopback_dsn_with_disable_is_accepted() {
        validate_postgres_transport_policy(
            "postgres://user:secret@127.42.0.7:5432/studiofs?sslmode=disable",
        )
        .expect("IPv4 loopback sslmode=disable should be accepted");

        validate_postgres_transport_policy(
            "host=::1 port=5432 user=studiofs password=secret sslmode=disable",
        )
        .expect("IPv6 loopback sslmode=disable should be accepted");
    }

    #[test]
    fn ipv4_mapped_loopback_dsn_with_disable_is_accepted() {
        validate_postgres_transport_policy(
            "postgres://user:secret@[::ffff:127.0.0.1]:5432/studiofs?sslmode=disable",
        )
        .expect("IPv4-mapped loopback sslmode=disable should be accepted");
    }
}
