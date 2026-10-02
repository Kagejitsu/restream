//! Asynchronous RTMP/RTMPS publishing client.
//!
//! Connection sequence (all awaited inside [`PublishClient::connect`]):
//! TCP (+TLS) → RTMP handshake → `connect(app)` → `createStream` →
//! `publish(key, live)`. After that the client is "publishing" and media can
//! be pushed with the non-blocking `try_send_*` methods.
//!
//! Two background tasks run per connection:
//! - **reader**: pulls bytes from the socket, feeds the `ClientSession`, and
//!   forwards any protocol responses it generates (acks, pong) to the writer
//!   via the control channel. A read error or EOF marks the client closed.
//! - **writer**: drains the control channel (priority) and the bounded media
//!   channel to the socket. Media backpressure surfaces to the caller as
//!   [`SendError::Lagging`] from `try_send_*`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult, PublishRequestType,
    StreamMetadata,
};
use rml_rtmp::time::RtmpTimestamp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use tracing::{debug, trace, warn};

use crate::url::PublishUrl;

/// Tunables for a connection attempt.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Overall deadline for TCP+TLS+handshake+connect+publish.
    pub connect_timeout: Duration,
    /// Capacity of the outbound media queue, in packets. When full, `try_send_*`
    /// returns `Lagging`. ~2 s of 60 fps video + 48 kHz AAC ≈ 220 packets.
    pub media_queue_packets: usize,
    /// Verify the TLS certificate chain for rtmps://. Only disable for a local
    /// test server with a self-signed certificate.
    pub tls_verify: bool,
    /// Value for the `flashVer` connect property (what the server logs as the encoder).
    pub flash_version: String,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            media_queue_packets: 512,
            tls_verify: true,
            flash_version: "FMLE/3.0 (compatible; restream)".to_string(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("DNS/TCP connect failed: {0}")]
    Tcp(std::io::Error),
    #[error("TLS handshake failed: {0}")]
    Tls(std::io::Error),
    #[error("invalid TLS server name {0}")]
    TlsName(String),
    #[error("RTMP handshake failed: {0:?}")]
    Handshake(rml_rtmp::handshake::HandshakeError),
    #[error("RTMP session error: {0:?}")]
    Session(rml_rtmp::sessions::ClientSessionError),
    #[error("server rejected connection: {0}")]
    Rejected(String),
    #[error("connection closed during setup")]
    Closed,
    #[error("timed out during connection setup")]
    Timeout,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SendError {
    /// The outbound media queue is full: the network cannot keep up.
    #[error("destination is lagging (send queue full)")]
    Lagging,
    /// The connection is gone; reconnect.
    #[error("connection closed")]
    Closed,
}

/// Counters shared with the owner for statistics.
#[derive(Debug, Default)]
pub struct Counters {
    pub bytes_sent: AtomicU64,
    pub packets_sent: AtomicU64,
    pub bytes_received: AtomicU64,
}

/// Either a plain TCP stream or a TLS-wrapped one.
enum Transport {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Transport {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Transport::Plain(s) => s.read(buf).await,
            Transport::Tls(s) => s.read(buf).await,
        }
    }
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Transport::Plain(s) => s.write_all(buf).await,
            Transport::Tls(s) => s.write_all(buf).await,
        }
    }
    fn into_split(self) -> (Box<dyn tokio::io::AsyncRead + Send + Unpin>, Box<dyn tokio::io::AsyncWrite + Send + Unpin>) {
        match self {
            Transport::Plain(s) => {
                let (r, w) = s.into_split();
                (Box::new(r), Box::new(w))
            }
            Transport::Tls(s) => {
                let (r, w) = tokio::io::split(*s);
                (Box::new(r), Box::new(w))
            }
        }
    }
}

/// A live publishing connection to one RTMP destination.
pub struct PublishClient {
    session: Arc<Mutex<ClientSession>>,
    media_tx: mpsc::Sender<Bytes>,
    ctrl_tx: mpsc::UnboundedSender<Bytes>,
    closed_rx: watch::Receiver<Option<String>>,
    counters: Arc<Counters>,
    url: PublishUrl,
}

impl PublishClient {
    /// Connect and complete the publish handshake. On success the returned
    /// client is ready for `try_send_*`.
    pub async fn connect(url: &PublishUrl, opts: &ConnectOptions) -> Result<Self, ConnectError> {
        match timeout(opts.connect_timeout, Self::connect_inner(url, opts)).await {
            Ok(r) => r,
            Err(_) => Err(ConnectError::Timeout),
        }
    }

    async fn connect_inner(url: &PublishUrl, opts: &ConnectOptions) -> Result<Self, ConnectError> {
        // --- transport ---
        let tcp = TcpStream::connect((url.host.as_str(), url.port)).await.map_err(ConnectError::Tcp)?;
        tcp.set_nodelay(true).ok();
        let mut transport = if url.secure {
            let connector = tls_connector(opts.tls_verify);
            let name = rustls::pki_types::ServerName::try_from(url.host.clone())
                .map_err(|_| ConnectError::TlsName(url.host.clone()))?;
            let tls = connector.connect(name, tcp).await.map_err(ConnectError::Tls)?;
            Transport::Tls(Box::new(tls))
        } else {
            Transport::Plain(tcp)
        };

        // --- RTMP handshake ---
        let mut hs = Handshake::new(PeerType::Client);
        let p0p1 = hs.generate_outbound_p0_and_p1().map_err(ConnectError::Handshake)?;
        transport.write_all(&p0p1).await?;
        let mut buf = vec![0u8; 4096];
        let remaining = loop {
            let n = transport.read(&mut buf).await?;
            if n == 0 {
                return Err(ConnectError::Closed);
            }
            match hs.process_bytes(&buf[..n]).map_err(ConnectError::Handshake)? {
                HandshakeProcessResult::InProgress { response_bytes } => {
                    if !response_bytes.is_empty() {
                        transport.write_all(&response_bytes).await?;
                    }
                }
                HandshakeProcessResult::Completed { response_bytes, remaining_bytes } => {
                    if !response_bytes.is_empty() {
                        transport.write_all(&response_bytes).await?;
                    }
                    break remaining_bytes;
                }
            }
        };
        debug!(target: "rtmp", url = %url, "handshake complete");

        // --- session: connect → createStream/publish ---
        let mut cfg = ClientSessionConfig::new();
        cfg.tc_url = Some(url.tc_url());
        cfg.flash_version = opts.flash_version.clone();
        let (mut session, initial) = ClientSession::new(cfg).map_err(ConnectError::Session)?;
        let mut pending: Vec<ClientSessionResult> = initial;
        pending.push(session.request_connection(url.app.clone()).map_err(ConnectError::Session)?);
        let mut inbound: Vec<u8> = remaining;
        let mut connected = false;
        let mut publishing = false;

        loop {
            // Flush everything the session wants to say, collect events.
            let mut events = Vec::new();
            for r in pending.drain(..) {
                match r {
                    ClientSessionResult::OutboundResponse(p) => transport.write_all(&p.bytes).await?,
                    ClientSessionResult::RaisedEvent(e) => events.push(e),
                    ClientSessionResult::UnhandleableMessageReceived(m) => {
                        trace!(target: "rtmp", "unhandled message during setup: {m:?}")
                    }
                }
            }
            for e in events {
                match e {
                    ClientSessionEvent::ConnectionRequestAccepted => {
                        connected = true;
                        let r = session
                            .request_publishing(url.stream_key.clone(), PublishRequestType::Live)
                            .map_err(ConnectError::Session)?;
                        pending.push(r);
                    }
                    ClientSessionEvent::ConnectionRequestRejected { description } => {
                        return Err(ConnectError::Rejected(description));
                    }
                    ClientSessionEvent::PublishRequestAccepted => publishing = true,
                    ClientSessionEvent::UnhandleableOnStatusCode { code } => {
                        if code.contains("Failed") || code.contains("BadName") || code.contains("Rejected") {
                            return Err(ConnectError::Rejected(code));
                        }
                        debug!(target: "rtmp", "onStatus during setup: {code}");
                    }
                    other => trace!(target: "rtmp", "setup event {other:?}"),
                }
            }
            if publishing {
                break;
            }
            if !pending.is_empty() {
                continue;
            }
            // Need more bytes from the server.
            if inbound.is_empty() {
                let n = transport.read(&mut buf).await?;
                if n == 0 {
                    return Err(ConnectError::Closed);
                }
                inbound.extend_from_slice(&buf[..n]);
            }
            let chunk = std::mem::take(&mut inbound);
            pending = session.handle_input(&chunk).map_err(ConnectError::Session)?;
        }
        let _ = connected;
        debug!(target: "rtmp", url = %url, "publishing");

        // --- spawn reader/writer ---
        let session = Arc::new(Mutex::new(session));
        let counters = Arc::new(Counters::default());
        let (media_tx, media_rx) = mpsc::channel::<Bytes>(opts.media_queue_packets.max(16));
        let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel::<Bytes>();
        let (closed_tx, closed_rx) = watch::channel::<Option<String>>(None);
        let (reader, writer) = transport.into_split();

        tokio::spawn(writer_task(writer, ctrl_rx, media_rx, Arc::clone(&counters), closed_tx.clone()));
        tokio::spawn(reader_task(reader, Arc::clone(&session), ctrl_tx.clone(), Arc::clone(&counters), closed_tx));

        Ok(Self { session, media_tx, ctrl_tx, closed_rx, counters, url: url.clone() })
    }

    pub fn url(&self) -> &PublishUrl {
        &self.url
    }

    pub fn counters(&self) -> &Arc<Counters> {
        &self.counters
    }

    /// `Some(reason)` once the connection has died.
    pub fn closed_reason(&self) -> Option<String> {
        self.closed_rx.borrow().clone()
    }

    pub fn is_closed(&self) -> bool {
        self.closed_rx.borrow().is_some()
    }

    /// Resolves when the connection dies (or immediately if it already has).
    pub async fn wait_closed(&self) -> String {
        let mut rx = self.closed_rx.clone();
        loop {
            if let Some(r) = rx.borrow().clone() {
                return r;
            }
            if rx.changed().await.is_err() {
                return "writer task gone".to_string();
            }
        }
    }

    /// Send `onMetaData`. Goes through the control channel so it is never dropped.
    pub fn send_metadata(&self, meta: &StreamMetadata) -> Result<(), SendError> {
        let pkt = {
            let mut s = self.session.lock();
            s.publish_metadata(meta).map_err(|_| SendError::Closed)?
        };
        self.push_ctrl(pkt)
    }

    /// Queue a video tag body (see [`crate::flv`]) with an absolute timestamp in ms.
    /// `droppable` marks inter frames the chunk layer may discard under pressure.
    pub fn try_send_video(&self, body: Bytes, ts_ms: u32, droppable: bool) -> Result<(), SendError> {
        let pkt = {
            let mut s = self.session.lock();
            s.publish_video_data(body, RtmpTimestamp::new(ts_ms), droppable).map_err(|_| SendError::Closed)?
        };
        self.push_media(pkt)
    }

    /// Queue an audio tag body with an absolute timestamp in ms.
    pub fn try_send_audio(&self, body: Bytes, ts_ms: u32) -> Result<(), SendError> {
        let pkt = {
            let mut s = self.session.lock();
            s.publish_audio_data(body, RtmpTimestamp::new(ts_ms), false).map_err(|_| SendError::Closed)?
        };
        self.push_media(pkt)
    }

    /// Sequence headers (AVC config / AAC config) must never be dropped, so they
    /// travel on the control channel like metadata.
    pub fn send_video_config(&self, body: Bytes, ts_ms: u32) -> Result<(), SendError> {
        let pkt = {
            let mut s = self.session.lock();
            s.publish_video_data(body, RtmpTimestamp::new(ts_ms), false).map_err(|_| SendError::Closed)?
        };
        self.push_ctrl(pkt)
    }

    pub fn send_audio_config(&self, body: Bytes, ts_ms: u32) -> Result<(), SendError> {
        let pkt = {
            let mut s = self.session.lock();
            s.publish_audio_data(body, RtmpTimestamp::new(ts_ms), false).map_err(|_| SendError::Closed)?
        };
        self.push_ctrl(pkt)
    }

    /// How many media packets are waiting for the socket (0 = keeping up).
    pub fn media_backlog(&self) -> usize {
        self.media_tx.max_capacity() - self.media_tx.capacity()
    }

    /// Politely stop publishing and drop the connection. Waits (bounded) for
    /// queued media to reach the socket first, so the final frames are not
    /// cut off by the unpublish command, which travels on the priority lane.
    pub async fn close(self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while self.media_backlog() > 0 && !self.is_closed() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // The queue being empty only means the writer has *taken* the last
        // packet; give it and the server a moment before announcing the end.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let pkts = {
            let mut s = self.session.lock();
            s.stop_publishing().unwrap_or_default()
        };
        for r in pkts {
            if let ClientSessionResult::OutboundResponse(p) = r {
                let _ = self.ctrl_tx.send(Bytes::from(p.bytes));
            }
        }
        // Give the writer a moment to flush, then drop everything (closes socket).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    fn push_ctrl(&self, pkt: ClientSessionResult) -> Result<(), SendError> {
        match pkt {
            ClientSessionResult::OutboundResponse(p) => {
                self.ctrl_tx.send(Bytes::from(p.bytes)).map_err(|_| SendError::Closed)
            }
            _ => Ok(()),
        }
    }

    fn push_media(&self, pkt: ClientSessionResult) -> Result<(), SendError> {
        if self.is_closed() {
            return Err(SendError::Closed);
        }
        match pkt {
            ClientSessionResult::OutboundResponse(p) => match self.media_tx.try_send(Bytes::from(p.bytes)) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => Err(SendError::Lagging),
                Err(mpsc::error::TrySendError::Closed(_)) => Err(SendError::Closed),
            },
            _ => Ok(()),
        }
    }
}

async fn writer_task(
    mut w: Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    mut ctrl_rx: mpsc::UnboundedReceiver<Bytes>,
    mut media_rx: mpsc::Receiver<Bytes>,
    counters: Arc<Counters>,
    closed_tx: watch::Sender<Option<String>>,
) {
    loop {
        // Prefer control packets (acks, pongs, config) when both are ready.
        let pkt = tokio::select! {
            biased;
            c = ctrl_rx.recv() => match c { Some(p) => p, None => break },
            m = media_rx.recv() => match m { Some(p) => p, None => break },
        };
        if let Err(e) = w.write_all(&pkt).await {
            let _ = closed_tx.send(Some(format!("write error: {e}")));
            return;
        }
        counters.bytes_sent.fetch_add(pkt.len() as u64, Ordering::Relaxed);
        counters.packets_sent.fetch_add(1, Ordering::Relaxed);
    }
    let _ = closed_tx.send(Some("client dropped".to_string()));
}

async fn reader_task(
    mut r: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    session: Arc<Mutex<ClientSession>>,
    ctrl_tx: mpsc::UnboundedSender<Bytes>,
    counters: Arc<Counters>,
    closed_tx: watch::Sender<Option<String>>,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match r.read(&mut buf).await {
            Ok(0) => {
                let _ = closed_tx.send(Some("server closed connection".to_string()));
                return;
            }
            Ok(n) => n,
            Err(e) => {
                let _ = closed_tx.send(Some(format!("read error: {e}")));
                return;
            }
        };
        counters.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
        let results = {
            let mut s = session.lock();
            match s.handle_input(&buf[..n]) {
                Ok(r) => r,
                Err(e) => {
                    let _ = closed_tx.send(Some(format!("protocol error: {e:?}")));
                    return;
                }
            }
        };
        for res in results {
            match res {
                ClientSessionResult::OutboundResponse(p) => {
                    if ctrl_tx.send(Bytes::from(p.bytes)).is_err() {
                        return;
                    }
                }
                ClientSessionResult::RaisedEvent(ClientSessionEvent::UnhandleableOnStatusCode { code }) => {
                    warn!(target: "rtmp", "server onStatus: {code}");
                    if code.contains("Failed") || code.contains("Unpublish") || code.contains("Stop") {
                        let _ = closed_tx.send(Some(format!("server status {code}")));
                        return;
                    }
                }
                ClientSessionResult::RaisedEvent(e) => trace!(target: "rtmp", "event {e:?}"),
                ClientSessionResult::UnhandleableMessageReceived(m) => trace!(target: "rtmp", "msg {m:?}"),
            }
        }
    }
}

fn tls_connector(verify: bool) -> tokio_rustls::TlsConnector {
    let config = if verify {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()
    } else {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(danger::NoVerify))
            .with_no_client_auth()
    };
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

/// Certificate verifier that accepts anything. Only for self-signed local test servers.
mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};

    #[derive(Debug)]
    pub struct NoVerify;

    impl ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            rustls::crypto::ring::default_provider().signature_verification_algorithms.supported_schemes()
        }
    }
}
