//! Client configuration: timeouts, retry policy, proxy and TLS settings.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFrozenSet, PyTuple};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};

use crate::errors::{ErrorKind, NxError, NxResult};

// ---------------------------------------------------------------------------
// Timeouts
// ---------------------------------------------------------------------------

pub const DEFAULT_CONNECT_TIMEOUT: f64 = 5.0;
pub const DEFAULT_READ_TIMEOUT: f64 = 30.0;
pub const DEFAULT_WRITE_TIMEOUT: f64 = 30.0;
pub const DEFAULT_POOL_TIMEOUT: f64 = 10.0;

/// Resolved timeouts used by the transport. `None` means "wait forever".
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timeouts {
    pub connect: Option<Duration>,
    pub read: Option<Duration>,
    pub write: Option<Duration>,
    pub pool: Option<Duration>,
}

impl Timeouts {
    pub const NONE: Timeouts = Timeouts {
        connect: None,
        read: None,
        write: None,
        pool: None,
    };
}

fn to_duration(seconds: Option<f64>) -> Option<Duration> {
    seconds.map(Duration::from_secs_f64)
}

fn parse_seconds(value: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<f64>> {
    if value.is_none() {
        return Ok(None);
    }
    let seconds: f64 = value.extract().map_err(|_| {
        PyTypeError::new_err(format!(
            "{name} timeout must be a number of seconds or None, got {}",
            value
                .get_type()
                .name()
                .map(|n| n.to_string())
                .unwrap_or_default()
        ))
    })?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(PyValueError::new_err(format!(
            "{name} timeout must be a positive, finite number of seconds or None, got {seconds}"
        )));
    }
    Ok(Some(seconds))
}

/// Granular request timeouts, in seconds.
///
/// * `Timeout()` - safe defaults: connect=5, read=30, write=30, pool=10
/// * `Timeout(10)` - 10 seconds for every phase
/// * `Timeout(None)` - no timeouts at all
/// * `Timeout(10, connect=2)` / `Timeout(read=60)` - override individual phases
#[pyclass(frozen, module = "nextreq", name = "Timeout")]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timeout {
    connect: Option<f64>,
    read: Option<f64>,
    write: Option<f64>,
    pool: Option<f64>,
}

impl Timeout {
    pub fn timeouts(&self) -> Timeouts {
        Timeouts {
            connect: to_duration(self.connect),
            read: to_duration(self.read),
            write: to_duration(self.write),
            pool: to_duration(self.pool),
        }
    }
}

impl Default for Timeout {
    fn default() -> Self {
        Timeout {
            connect: Some(DEFAULT_CONNECT_TIMEOUT),
            read: Some(DEFAULT_READ_TIMEOUT),
            write: Some(DEFAULT_WRITE_TIMEOUT),
            pool: Some(DEFAULT_POOL_TIMEOUT),
        }
    }
}

fn fmt_seconds(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{v:?}"),
        None => "None".to_owned(),
    }
}

#[pymethods]
impl Timeout {
    #[new]
    #[pyo3(signature = (*args, **kwargs))]
    fn py_new(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        if args.len() > 1 {
            return Err(PyTypeError::new_err(format!(
                "Timeout() takes at most 1 positional argument ({} given)",
                args.len()
            )));
        }
        let mut base: Option<Bound<'_, PyAny>> = if args.len() == 1 {
            Some(args.get_item(0)?)
        } else {
            None
        };
        if let Some(kw) = kwargs {
            if let Some(value) = kw.get_item("timeout")? {
                if base.is_some() {
                    return Err(PyTypeError::new_err(
                        "Timeout() got multiple values for argument 'timeout'",
                    ));
                }
                base = Some(value);
            }
        }

        let mut out = Timeout::default();
        if let Some(value) = base {
            if let Ok(other) = value.cast::<Timeout>() {
                out = *other.get();
            } else {
                let seconds = parse_seconds(&value, "timeout")?;
                out = Timeout {
                    connect: seconds,
                    read: seconds,
                    write: seconds,
                    pool: seconds,
                };
            }
        }
        if let Some(kw) = kwargs {
            for (key, value) in kw.iter() {
                let key: String = key.extract()?;
                match key.as_str() {
                    "timeout" => {}
                    "connect" => out.connect = parse_seconds(&value, "connect")?,
                    "read" => out.read = parse_seconds(&value, "read")?,
                    "write" => out.write = parse_seconds(&value, "write")?,
                    "pool" => out.pool = parse_seconds(&value, "pool")?,
                    other => {
                        return Err(PyTypeError::new_err(format!(
                            "Timeout() got an unexpected keyword argument '{other}'"
                        )))
                    }
                }
            }
        }
        Ok(out)
    }

    /// Seconds allowed for DNS + TCP + TLS handshake.
    #[getter]
    fn connect(&self) -> Option<f64> {
        self.connect
    }

    /// Seconds allowed between reads (response headers, then each body chunk).
    #[getter]
    fn read(&self) -> Option<f64> {
        self.read
    }

    /// Seconds allowed without upload progress while sending the request.
    #[getter]
    fn write(&self) -> Option<f64> {
        self.write
    }

    /// Seconds allowed waiting for a free connection slot.
    #[getter]
    fn pool(&self) -> Option<f64> {
        self.pool
    }

    /// `(connect, read, write, pool)`.
    fn as_tuple(&self) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
        (self.connect, self.read, self.write, self.pool)
    }

    fn __repr__(&self) -> String {
        format!(
            "Timeout(connect={}, read={}, write={}, pool={})",
            fmt_seconds(self.connect),
            fmt_seconds(self.read),
            fmt_seconds(self.write),
            fmt_seconds(self.pool)
        )
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.cast::<Timeout>() {
            Ok(other) => self == other.get(),
            Err(_) => false,
        }
    }

    fn __hash__(&self) -> u64 {
        let bits = |v: Option<f64>| v.map(f64::to_bits).unwrap_or(u64::MAX);
        bits(self.connect)
            .wrapping_mul(31)
            .wrapping_add(bits(self.read))
            .wrapping_mul(31)
            .wrapping_add(bits(self.write))
            .wrapping_mul(31)
            .wrapping_add(bits(self.pool))
    }
}

// ---------------------------------------------------------------------------
// Retry policy
// ---------------------------------------------------------------------------

pub const DEFAULT_RETRY_STATUSES: [u16; 4] = [429, 502, 503, 504];
pub const DEFAULT_RETRY_METHODS: [&str; 6] = ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"];

/// Exponential backoff for retry number `attempt` (1-based), before jitter.
pub fn base_backoff(backoff_factor: f64, max_backoff: f64, attempt: u32) -> f64 {
    if attempt == 0 || backoff_factor <= 0.0 {
        return 0.0;
    }
    let exponent = (attempt - 1).min(62) as i32;
    (backoff_factor * 2f64.powi(exponent)).min(max_backoff)
}

/// Apply jitter: the result is drawn uniformly from `[base * (1 - jitter), base]`
/// where `unit` is a uniform random sample in `[0, 1)`.
pub fn apply_jitter(base: f64, jitter: f64, unit: f64) -> f64 {
    let jitter = jitter.clamp(0.0, 1.0);
    base * (1.0 - jitter * unit)
}

/// Parse an HTTP `Retry-After` header relative to `now`.
///
/// Accepts delta-seconds (`"120"`) and HTTP-dates
/// (`"Wed, 21 Oct 2015 07:28:00 GMT"`). Dates in the past yield `0.0`.
pub fn parse_retry_after_at(value: &str, now: SystemTime) -> Option<f64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.bytes().all(|b| b.is_ascii_digit()) {
        return value.parse::<u64>().ok().map(|s| s as f64);
    }
    if let Ok(seconds) = value.parse::<f64>() {
        // Not strictly RFC 9110, but some servers send fractional seconds.
        if seconds.is_finite() && seconds >= 0.0 {
            return Some(seconds);
        }
        return None;
    }
    let when = httpdate::parse_http_date(value).ok()?;
    Some(match when.duration_since(now) {
        Ok(delta) => delta.as_secs_f64(),
        Err(_) => 0.0,
    })
}

/// Parse a `Retry-After` header value into seconds from now.
///
/// Returns `None` if the value is neither delta-seconds nor an HTTP-date.
#[pyfunction]
pub fn parse_retry_after(value: &str) -> Option<f64> {
    parse_retry_after_at(value, SystemTime::now())
}

/// Declarative retry policy.
///
/// `max_attempts` is the maximum number of *retries* after the initial
/// request, so `RetryPolicy(max_attempts=3)` sends at most 4 requests.
#[pyclass(frozen, module = "nextreq", name = "RetryPolicy")]
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    max_attempts: u32,
    backoff_factor: f64,
    max_backoff: f64,
    jitter: f64,
    retry_statuses: Vec<u16>,
    retry_methods: Vec<String>,
    respect_retry_after: bool,
    max_retry_after: f64,
}

impl RetryPolicy {
    /// Seconds to wait before retry number `attempt`, given a random sample.
    pub fn delay_with(&self, attempt: u32, retry_after: Option<&str>, unit: f64) -> f64 {
        if self.respect_retry_after {
            if let Some(seconds) = retry_after.and_then(parse_retry_after) {
                return seconds.min(self.max_retry_after);
            }
        }
        let base = base_backoff(self.backoff_factor, self.max_backoff, attempt);
        apply_jitter(base, self.jitter, unit)
    }
}

fn collect_statuses(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<u16>> {
    let mut out: Vec<u16> = match value {
        None => DEFAULT_RETRY_STATUSES.to_vec(),
        Some(v) if v.is_none() => DEFAULT_RETRY_STATUSES.to_vec(),
        Some(v) => {
            let mut items = Vec::new();
            for item in v.try_iter()? {
                let code: u16 = item?.extract()?;
                if !(100..=599).contains(&code) {
                    return Err(PyValueError::new_err(format!(
                        "invalid HTTP status code in retry_statuses: {code}"
                    )));
                }
                items.push(code);
            }
            items
        }
    };
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

fn collect_methods(value: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let defaults = || {
        DEFAULT_RETRY_METHODS
            .iter()
            .map(|m| (*m).to_owned())
            .collect()
    };
    let mut out: Vec<String> = match value {
        None => defaults(),
        Some(v) if v.is_none() => defaults(),
        Some(v) => {
            if v.extract::<String>().is_ok() {
                return Err(PyTypeError::new_err(
                    "retry_methods must be an iterable of method names, not a single string",
                ));
            }
            let mut items = Vec::new();
            for item in v.try_iter()? {
                let method: String = item?.extract()?;
                items.push(method.to_ascii_uppercase());
            }
            items
        }
    };
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

#[pymethods]
impl RetryPolicy {
    #[new]
    #[pyo3(signature = (
        max_attempts = 3,
        *,
        backoff_factor = 0.5,
        max_backoff = 30.0,
        jitter = None,
        retry_statuses = None,
        retry_methods = None,
        respect_retry_after = true,
        max_retry_after = 120.0,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn py_new(
        max_attempts: u32,
        backoff_factor: f64,
        max_backoff: f64,
        jitter: Option<&Bound<'_, PyAny>>,
        retry_statuses: Option<&Bound<'_, PyAny>>,
        retry_methods: Option<&Bound<'_, PyAny>>,
        respect_retry_after: bool,
        max_retry_after: f64,
    ) -> PyResult<Self> {
        if !backoff_factor.is_finite() || backoff_factor < 0.0 {
            return Err(PyValueError::new_err("backoff_factor must be >= 0"));
        }
        if !max_backoff.is_finite() || max_backoff < 0.0 {
            return Err(PyValueError::new_err("max_backoff must be >= 0"));
        }
        if !max_retry_after.is_finite() || max_retry_after < 0.0 {
            return Err(PyValueError::new_err("max_retry_after must be >= 0"));
        }
        let jitter = match jitter {
            None => 1.0,
            Some(v) if v.is_none() => 1.0,
            Some(v) => {
                if let Ok(flag) = v.cast::<pyo3::types::PyBool>() {
                    if flag.is_true() {
                        1.0
                    } else {
                        0.0
                    }
                } else {
                    let fraction: f64 = v.extract().map_err(|_| {
                        PyTypeError::new_err("jitter must be a bool or a float in [0, 1]")
                    })?;
                    if !(0.0..=1.0).contains(&fraction) {
                        return Err(PyValueError::new_err("jitter must be within [0, 1]"));
                    }
                    fraction
                }
            }
        };
        Ok(RetryPolicy {
            max_attempts,
            backoff_factor,
            max_backoff,
            jitter,
            retry_statuses: collect_statuses(retry_statuses)?,
            retry_methods: collect_methods(retry_methods)?,
            respect_retry_after,
            max_retry_after,
        })
    }

    /// Maximum number of retries after the initial attempt.
    #[getter]
    fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    #[getter]
    fn backoff_factor(&self) -> f64 {
        self.backoff_factor
    }

    #[getter]
    fn max_backoff(&self) -> f64 {
        self.max_backoff
    }

    /// Fraction of each backoff delay that is randomised (0 = none, 1 = full).
    #[getter]
    fn jitter(&self) -> f64 {
        self.jitter
    }

    #[getter]
    fn retry_statuses<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyFrozenSet>> {
        PyFrozenSet::new(py, self.retry_statuses.iter().copied())
    }

    #[getter]
    fn retry_methods<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyFrozenSet>> {
        PyFrozenSet::new(py, self.retry_methods.iter().map(String::as_str))
    }

    #[getter]
    fn respect_retry_after(&self) -> bool {
        self.respect_retry_after
    }

    #[getter]
    fn max_retry_after(&self) -> f64 {
        self.max_retry_after
    }

    /// Whether requests using `method` may be retried.
    fn allows_method(&self, method: &str) -> bool {
        self.retry_methods
            .iter()
            .any(|m| m.eq_ignore_ascii_case(method))
    }

    /// Whether a response with `status` should be retried.
    fn allows_status(&self, status: u16) -> bool {
        self.retry_statuses.binary_search(&status).is_ok()
    }

    /// Seconds to sleep before retry number `attempt` (1-based).
    ///
    /// A parseable `Retry-After` header takes precedence over the computed
    /// exponential backoff (capped at `max_retry_after`).
    #[pyo3(signature = (attempt, retry_after = None))]
    fn delay(&self, attempt: u32, retry_after: Option<&str>) -> f64 {
        self.delay_with(attempt, retry_after, fastrand::f64())
    }

    fn __repr__(&self) -> String {
        format!(
            "RetryPolicy(max_attempts={}, backoff_factor={:?}, max_backoff={:?}, jitter={:?}, \
             retry_statuses={:?}, retry_methods={:?}, respect_retry_after={}, max_retry_after={:?})",
            self.max_attempts,
            self.backoff_factor,
            self.max_backoff,
            self.jitter,
            self.retry_statuses,
            self.retry_methods,
            if self.respect_retry_after { "True" } else { "False" },
            self.max_retry_after,
        )
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.cast::<RetryPolicy>() {
            Ok(other) => self == other.get(),
            Err(_) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

/// TLS settings shared by the HTTP engine and the WebSocket client.
#[derive(Debug, Clone, Default)]
pub struct TlsOptions {
    /// Verify server certificates (default true).
    pub verify: bool,
    /// PEM bundle to trust instead of the default roots.
    pub ca_bundle: Option<String>,
    /// PEM client certificate chain.
    pub cert: Option<String>,
    /// PEM private key (defaults to the `cert` file).
    pub key: Option<String>,
}

fn crypto_provider() -> Arc<CryptoProvider> {
    static PROVIDER: OnceLock<Arc<CryptoProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| Arc::new(rustls::crypto::ring::default_provider()))
        .clone()
}

/// Mozilla's root set (`webpki-roots`) merged with the OS trust store
/// (`rustls-native-certs`, which also honours `SSL_CERT_FILE`).
fn default_root_store() -> RootCertStore {
    let mut store = RootCertStore::empty();
    store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        // Individual malformed platform certificates are skipped.
        let _ = store.add(cert);
    }
    store
}

/// Verifies against the default roots, loading them on first use.
///
/// Reading the OS trust store can take tens of milliseconds, so it is deferred
/// until the first TLS handshake (which runs on a Tokio worker, GIL-free) and
/// shared by every client in the process.
#[derive(Debug)]
struct DefaultRootsVerifier {
    provider: Arc<CryptoProvider>,
}

fn shared_webpki_verifier(
    provider: &Arc<CryptoProvider>,
) -> Result<Arc<WebPkiServerVerifier>, rustls::Error> {
    static VERIFIER: OnceLock<Result<Arc<WebPkiServerVerifier>, String>> = OnceLock::new();
    VERIFIER
        .get_or_init(|| {
            WebPkiServerVerifier::builder_with_provider(
                Arc::new(default_root_store()),
                provider.clone(),
            )
            .build()
            .map_err(|e| e.to_string())
        })
        .clone()
        .map_err(rustls::Error::General)
}

impl ServerCertVerifier for DefaultRootsVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        shared_webpki_verifier(&self.provider)?.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Accepts any certificate. Only used for `verify=False`.
#[derive(Debug)]
struct NoVerification {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn tls_error(context: &str, err: impl std::fmt::Display) -> NxError {
    NxError::new(ErrorKind::LocalProtocolError, format!("{context}: {err}"))
}

fn load_certs(path: &str) -> NxResult<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|e| tls_error(&format!("could not read certificates from {path:?}"), e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| tls_error(&format!("invalid PEM certificate in {path:?}"), e))?;
    if certs.is_empty() {
        return Err(tls_error(
            "TLS configuration error",
            format!("no PEM certificates found in {path:?}"),
        ));
    }
    Ok(certs)
}

/// Build a rustls client configuration. `alpn` lists the protocols to offer.
pub fn build_tls_config(options: &TlsOptions, alpn: &[&[u8]]) -> NxResult<ClientConfig> {
    let provider = crypto_provider();
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| tls_error("TLS configuration error", e))?;

    let builder = if !options.verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerification { provider }))
    } else if let Some(path) = &options.ca_bundle {
        let mut store = RootCertStore::empty();
        for cert in load_certs(path)? {
            store
                .add(cert)
                .map_err(|e| tls_error(&format!("invalid CA certificate in {path:?}"), e))?;
        }
        builder.with_root_certificates(store)
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(DefaultRootsVerifier { provider }))
    };

    let mut config = match &options.cert {
        Some(cert_path) => {
            let chain = load_certs(cert_path)?;
            let key_path = options.key.as_deref().unwrap_or(cert_path);
            let key = PrivateKeyDer::from_pem_file(key_path).map_err(|e| {
                tls_error(&format!("could not read private key from {key_path:?}"), e)
            })?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| tls_error("invalid client certificate", e))?
        }
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(config)
}

/// Whether `domain` is a public suffix - a name under which unrelated parties
/// register domains, such as `com`, `co.uk` or `github.io` - according to the
/// Public Suffix List compiled into this build.
///
/// Following the list's default rule, a single label that is not listed
/// (`localhost`, `internal`) counts as a public suffix. IP addresses do not.
#[pyfunction]
pub fn is_public_suffix(domain: &str) -> bool {
    let domain = domain.trim_matches('.').to_ascii_lowercase();
    if domain.is_empty() || domain.parse::<std::net::IpAddr>().is_ok() {
        return false;
    }
    psl::suffix(domain.as_bytes()).is_some_and(|suffix| suffix.as_bytes().len() == domain.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(base_backoff(0.5, 30.0, 0), 0.0);
        assert_eq!(base_backoff(0.5, 30.0, 1), 0.5);
        assert_eq!(base_backoff(0.5, 30.0, 2), 1.0);
        assert_eq!(base_backoff(0.5, 30.0, 3), 2.0);
        assert_eq!(base_backoff(0.5, 30.0, 4), 4.0);
        assert_eq!(base_backoff(0.5, 30.0, 10), 30.0);
        assert_eq!(base_backoff(0.5, 30.0, u32::MAX), 30.0);
        assert_eq!(base_backoff(0.0, 30.0, 5), 0.0);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        assert_eq!(apply_jitter(4.0, 0.0, 0.99), 4.0);
        assert_eq!(apply_jitter(4.0, 1.0, 0.0), 4.0);
        assert!((apply_jitter(4.0, 1.0, 0.5) - 2.0).abs() < 1e-9);
        assert!((apply_jitter(4.0, 0.5, 1.0) - 2.0).abs() < 1e-9);
        for i in 0..100 {
            let unit = i as f64 / 100.0;
            let d = apply_jitter(8.0, 0.25, unit);
            assert!((6.0..=8.0).contains(&d));
        }
    }

    #[test]
    fn retry_after_delta_seconds() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        assert_eq!(parse_retry_after_at("120", now), Some(120.0));
        assert_eq!(parse_retry_after_at("  7 ", now), Some(7.0));
        assert_eq!(parse_retry_after_at("0", now), Some(0.0));
        assert_eq!(parse_retry_after_at("1.5", now), Some(1.5));
    }

    #[test]
    fn retry_after_http_date() {
        // Wed, 21 Oct 2015 07:28:00 GMT == 1445412480
        let now = UNIX_EPOCH + Duration::from_secs(1_445_412_480 - 90);
        assert_eq!(
            parse_retry_after_at("Wed, 21 Oct 2015 07:28:00 GMT", now),
            Some(90.0)
        );
        // Dates in the past mean "retry now".
        let later = UNIX_EPOCH + Duration::from_secs(1_445_412_480 + 500);
        assert_eq!(
            parse_retry_after_at("Wed, 21 Oct 2015 07:28:00 GMT", later),
            Some(0.0)
        );
    }

    #[test]
    fn retry_after_garbage_is_none() {
        let now = SystemTime::now();
        assert_eq!(parse_retry_after_at("", now), None);
        assert_eq!(parse_retry_after_at("soon", now), None);
        assert_eq!(parse_retry_after_at("-5", now), None);
        assert_eq!(parse_retry_after_at("nan", now), None);
    }

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            backoff_factor: 0.5,
            max_backoff: 30.0,
            jitter: 0.0,
            retry_statuses: DEFAULT_RETRY_STATUSES.to_vec(),
            retry_methods: DEFAULT_RETRY_METHODS
                .iter()
                .map(|m| (*m).to_owned())
                .collect(),
            respect_retry_after: true,
            max_retry_after: 120.0,
        }
    }

    #[test]
    fn delay_prefers_retry_after_and_caps_it() {
        let p = policy();
        assert_eq!(p.delay_with(1, None, 0.5), 0.5);
        assert_eq!(p.delay_with(3, None, 0.5), 2.0);
        assert_eq!(p.delay_with(1, Some("7"), 0.5), 7.0);
        assert_eq!(p.delay_with(1, Some("9999"), 0.5), 120.0);
        assert_eq!(p.delay_with(2, Some("garbage"), 0.5), 1.0);

        let ignoring = RetryPolicy {
            respect_retry_after: false,
            ..policy()
        };
        assert_eq!(ignoring.delay_with(2, Some("7"), 0.5), 1.0);
    }

    #[test]
    fn default_timeouts_are_safe() {
        let t = Timeout::default().timeouts();
        assert_eq!(t.connect, Some(Duration::from_secs(5)));
        assert_eq!(t.read, Some(Duration::from_secs(30)));
        assert_eq!(t.write, Some(Duration::from_secs(30)));
        assert_eq!(t.pool, Some(Duration::from_secs(10)));
        assert_eq!(Timeouts::NONE.connect, None);
    }

    #[test]
    fn tls_config_sets_alpn_and_rejects_missing_files() {
        let options = TlsOptions {
            verify: true,
            ..Default::default()
        };
        let config = build_tls_config(&options, &[b"h2", b"http/1.1"]).unwrap();
        assert_eq!(
            config.alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );

        let insecure = TlsOptions::default();
        assert!(build_tls_config(&insecure, &[b"http/1.1"]).is_ok());

        let missing = TlsOptions {
            verify: true,
            ca_bundle: Some("/definitely/not/a/real/bundle.pem".to_owned()),
            ..Default::default()
        };
        let err = build_tls_config(&missing, &[]).unwrap_err();
        assert_eq!(err.kind, ErrorKind::LocalProtocolError);
    }
}
