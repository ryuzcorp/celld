// Modified by ryuzcorp (ryuzcorp/celld): tenant egress policy; reaction-aware request resources; TCP socket ownership.
//! Outbound TCP host ops for `cloudflare:sockets` `connect()`.
//!
//! A socket is a split tokio stream in a process-global registry: the
//! read and write halves live behind separate async mutexes so a
//! pending read never blocks a write. `startTls()` takes both halves
//! back, reunites them, runs the rustls handshake, and registers the
//! upgraded stream under a new id — the old id is gone, which is what
//! makes the old Socket's streams fail instead of leaking plaintext.
//!
//! Every op is an `asyncrt` promise. The ops do their own tokio I/O
//! instead of going through a connector task: unlike an outbound
//! WebSocket, a TCP socket never outlives the event that created it
//! (the request context frees it at retirement), so nothing has to
//! survive the request driver.
//!
//! Script names sockets by number (`Socket._id` is a public mutable field),
//! so the ids are unguessable and each entry records its owning event
//! context: every op answers a foreign id exactly as a missing one.

use super::*;
use rand::RngCore;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

type Duplex = Box<dyn Stream>;
type ReadHalf = tokio::io::ReadHalf<Duplex>;
type WriteHalf = tokio::io::WriteHalf<Duplex>;

struct TcpSocket {
    read: Arc<tokio::sync::Mutex<Option<ReadHalf>>>,
    write: Arc<tokio::sync::Mutex<Option<WriteHalf>>>,
    /// The event context that opened the socket, held weakly so ownership
    /// never extends the event. A retired event owns nothing.
    owner: Weak<IoContext>,
}

fn registry() -> &'static Mutex<HashMap<u64, TcpSocket>> {
    static SOCKETS: OnceLock<Mutex<HashMap<u64, TcpSocket>>> = OnceLock::new();
    SOCKETS.get_or_init(Default::default)
}

fn next_id() -> u64 {
    // Unguessable ids in the same f64-safe nonzero space as WebSocket ids:
    // a sequential counter lets any event drive any socket by reassignment.
    const F64_SAFE: u64 = 0x1FFF_FFFF_FFFF; // 2^53 - 1
    loop {
        let id = rand::rngs::OsRng.next_u64() & F64_SAFE;
        if id != 0 && !registry().lock().unwrap().contains_key(&id) {
            return id;
        }
    }
}

/// Drop one socket's registry entry, which closes the connection. The
/// request context calls this for every socket the event opened, so an
/// abandoned socket cannot outlive its event. Idempotent.
pub(super) fn free(id: u64) {
    registry().lock().unwrap().remove(&id);
}

fn insert(id: u64, stream: Duplex, owner: Weak<IoContext>) {
    let (read, write) = tokio::io::split(stream);
    registry().lock().unwrap().insert(
        id,
        TcpSocket {
            read: Arc::new(tokio::sync::Mutex::new(Some(read))),
            write: Arc::new(tokio::sync::Mutex::new(Some(write))),
            owner,
        },
    );
}

/// Whether `owner` still names the running event's context: a live weak
/// reference to exactly this event. Anything else — another event, a retired
/// event — owns nothing here.
fn owns(owner: &Weak<IoContext>, context: &Arc<IoContext>) -> bool {
    owner
        .upgrade()
        .is_some_and(|owner| Arc::ptr_eq(&owner, context))
}

/// Mozilla's roots, the same choice `ws_client` makes and for the same
/// reason: a downloaded celld must reach TLS hosts on a machine with no
/// `/etc/ssl/certs`. Test TLS servers use a root injected through the
/// gated seam below.
#[cfg(celld_internal_tests)]
fn test_root() -> &'static Mutex<Option<Vec<u8>>> {
    static ROOT: OnceLock<Mutex<Option<Vec<u8>>>> = OnceLock::new();
    ROOT.get_or_init(Default::default)
}

/// Trust one extra DER root for outbound TLS, so test servers with
/// self-signed certificates verify. No compatibility guarantee.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn test_extra_tls_root(der: Vec<u8>) {
    *test_root().lock().unwrap() = Some(der);
}

// Rebuilt per handshake only under the internal-tests cfg, where the
// injected root must be visible; production caches the connector.
#[cfg(celld_internal_tests)]
fn tls_connector() -> tokio_rustls::TlsConnector {
    build_tls_connector(test_root().lock().unwrap().clone())
}

#[cfg(not(celld_internal_tests))]
fn tls_connector() -> tokio_rustls::TlsConnector {
    static CONNECTOR: OnceLock<tokio_rustls::TlsConnector> = OnceLock::new();
    CONNECTOR.get_or_init(|| build_tls_connector(None)).clone()
}

fn build_tls_connector(extra_root: Option<Vec<u8>>) -> tokio_rustls::TlsConnector {
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    if let Some(der) = extra_root {
        let _ = roots.add(rustls::pki_types::CertificateDer::from(der));
    }
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

async fn handshake(stream: Duplex, host: String) -> Result<Duplex, String> {
    let name = rustls::pki_types::ServerName::try_from(host)
        .map_err(|error| format!("invalid TLS server name: {error}"))?;
    let tls = tls_connector()
        .connect(name, stream)
        .await
        .map_err(|error| format!("TLS handshake failed: {error}"))?;
    Ok(Box::new(tls))
}

#[derive(serde::Deserialize)]
struct ConnectArgs {
    hostname: String,
    port: u16,
    secure: bool,
    #[serde(default)]
    expected_host: Option<String>,
}

pub(super) fn op_tcp_connect(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    match &actor_runtime_state(scope).egress {
        EgressPolicy::Allow => {}
        EgressPolicy::Deny => {
            return loader_throw(
                scope,
                "This worker is not permitted to access the internet via global functions.",
            );
        }
        // A Fetcher is still an authority boundary for sockets. celld's
        // service protocol has no bidirectional CONNECT tunnel, so refusing is
        // required: a direct TcpStream here would silently bypass the broker.
        EgressPolicy::Broker(_) => {
            return loader_throw(
                scope,
                "A globalOutbound Fetcher cannot broker connect() in celld.",
            );
        }
    }
    let raw = args.get(0).to_rust_string_lossy(scope);
    let request: ConnectArgs = match serde_json::from_str(&raw) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("connect(): {error}")),
    };
    // The owner is the running entry's request: a continuation resumed in a
    // foreign checkpoint must account its socket to its own event. Resolving
    // the turn owner instead lets the turn owner's retirement close a live
    // socket — or, mid-connect, insert it with no owner at all. Check first:
    // a retired owner throws with nothing pushed.
    let Some(event) = event_context(scope) else {
        return retired_event_throw(scope);
    };
    // The id exists before the connection does, so the request context
    // can own the socket even if the event ends mid-connect.
    let id = next_id();
    event.tcp_sockets.lock().unwrap().push(id);
    let owner = Arc::downgrade(&event);
    let gate = egress_gate_request(&event, celld_logic::Channel::Tcp);
    let async_id =
        asyncrt::enqueue(async move {
            await_egress_gate(gate).await?;
            // The tenant egress policy judges the resolved address before
            // the dial; open mode dials exactly as before.
            let policy = crate::egress::EgressPolicy::global();
            let stream =
                match crate::egress::policed_tcp_connect(&policy, &request.hostname, request.port)
                    .await
                {
                    Ok(stream) => stream,
                    Err(error) => {
                        let detail = match error {
                            crate::egress::DialError::Refused(refusal) => refusal.to_string(),
                            crate::egress::DialError::Resolve(error) => error,
                            crate::egress::DialError::Tcp(error) => error,
                        };
                        return Err(format!(
                            "connection to {}:{} failed: {detail}",
                            request.hostname, request.port
                        ));
                    }
                };
            let remote = stream.peer_addr().map(|a| a.to_string()).ok();
            let local = stream.local_addr().map(|a| a.to_string()).ok();
            let mut stream: Duplex = Box::new(stream);
            if request.secure {
                let host = request.expected_host.unwrap_or(request.hostname);
                stream = handshake(stream, host).await?;
            }
            insert(id, stream, owner);
            Ok::<String, String>(
                serde_json::json!({
                    "id": id,
                    "remoteAddress": remote,
                    "localAddress": local,
                })
                .to_string(),
            )
        });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    // The owner is checked under the same lock hold as the lookup, so the
    // entry cannot be freed and its id reused between the two. A foreign id
    // resolves as no half at all: "socket is closed", exactly as missing.
    let Some(event) = event_context(scope) else {
        return retired_event_throw(scope);
    };
    let half = registry()
        .lock()
        .unwrap()
        .get(&id)
        .filter(|s| owns(&s.owner, &event))
        .map(|s| s.read.clone());
    let async_id = asyncrt::enqueue(async move {
        let half = half.ok_or("socket is closed")?;
        let mut guard = half.lock().await;
        let read = guard.as_mut().ok_or("socket is closed")?;
        let mut buffer = vec![0u8; 16 * 1024];
        let n = read
            .read(&mut buffer)
            .await
            .map_err(|error| format!("read failed: {error}"))?;
        buffer.truncate(n);
        Ok::<Vec<u8>, String>(buffer)
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let Some(bytes) = view_bytes(args.get(1)) else {
        return loader_throw(scope, "socket write needs bytes");
    };
    let Some(event) = event_context(scope) else {
        return retired_event_throw(scope);
    };
    let half = registry()
        .lock()
        .unwrap()
        .get(&id)
        .filter(|s| owns(&s.owner, &event))
        .map(|s| s.write.clone());
    let gate = egress_gate_request(&event, celld_logic::Channel::Tcp);
    let async_id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        let half = half.ok_or("socket is closed")?;
        let mut guard = half.lock().await;
        let write = guard.as_mut().ok_or("socket is closed")?;
        write
            .write_all(&bytes)
            .await
            .map_err(|error| format!("write failed: {error}"))?;
        write
            .flush()
            .await
            .map_err(|error| format!("flush failed: {error}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_shutdown(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let Some(event) = event_context(scope) else {
        return retired_event_throw(scope);
    };
    let half = registry()
        .lock()
        .unwrap()
        .get(&id)
        .filter(|s| owns(&s.owner, &event))
        .map(|s| s.write.clone());
    let gate = egress_gate_request(&event, celld_logic::Channel::Tcp);
    let async_id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        let half = half.ok_or("socket is closed")?;
        let mut guard = half.lock().await;
        let write = guard.as_mut().ok_or("socket is closed")?;
        write
            .shutdown()
            .await
            .map_err(|error| format!("shutdown failed: {error}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_tcp_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    // A foreign id is left alone — closing it would drop another event's
    // connection — and a missing id was already gone. Both are silent.
    let Some(event) = event_context(scope) else {
        return retired_event_throw(scope);
    };
    let mut registry = registry().lock().unwrap();
    if registry.get(&id).is_some_and(|s| owns(&s.owner, &event)) {
        registry.remove(&id);
    }
}

#[derive(serde::Deserialize)]
struct StartTlsArgs {
    id: u64,
    host: String,
}

pub(super) fn op_tcp_starttls(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let raw = args.get(0).to_rust_string_lossy(scope);
    let request: StartTlsArgs = match serde_json::from_str(&raw) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("startTls(): {error}")),
    };
    // As for `connect`: the running entry owns the upgraded socket, and a
    // retired owner throws before anything is pushed.
    let Some(event) = event_context(scope) else {
        return retired_event_throw(scope);
    };
    let new_id = next_id();
    event.tcp_sockets.lock().unwrap().push(new_id);
    let owner = Arc::downgrade(&event);
    let gate = egress_gate_request(&event, celld_logic::Channel::Tcp);
    let async_id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        // The plaintext socket remains registered while the gate waits so the
        // event cleanup can close it. Consume it only when the TLS handshake can
        // start, and make every later operation on the old id fail. The owner
        // is checked under the same lock hold as the removal, so a foreign
        // id is refused exactly as a missing one — and can never be consumed.
        let taken = {
            let mut registry = registry().lock().unwrap();
            let owned = registry.get(&request.id).is_some_and(|socket| {
                socket.owner.upgrade().is_some_and(|held| {
                    owner
                        .upgrade()
                        .is_some_and(|mine| Arc::ptr_eq(&held, &mine))
                })
            });
            if owned {
                registry.remove(&request.id)
            } else {
                None
            }
        };
        let socket = taken.ok_or("socket is closed")?;
        let read = socket.read.lock().await.take().ok_or("socket is closed")?;
        let write = socket.write.lock().await.take().ok_or("socket is closed")?;
        let stream = read.unsplit(write);
        let stream = handshake(stream, request.host).await?;
        insert(new_id, stream, owner);
        Ok::<String, String>(serde_json::json!({ "id": new_id }).to_string())
    });
    rv.set(promise_for(scope, async_id));
}
