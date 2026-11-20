// Modified by Ryuz Corp (Cello): capped WebSocket message assembly. Apache-2.0.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! One assembled WebSocket message, bounded.
//!
//! fastwebsockets 0.8.1 `FragmentCollector` (`FragmentCollectorRead`)
//! accumulates continuation-frame payloads with no total cap: its
//! `max_message_size` (64 MiB by default) applies per frame, so a peer that
//! sends a text/binary frame with FIN=0 followed by endless continuations
//! grows host memory without bound. Every socket that reads frames through
//! this module assembles through [`CappedFragmentReader`] instead, which
//! tracks the in-progress message and fails the read once the total passes
//! [`WS_MAX_MESSAGE_BYTES`]. The tenant-facing pumps end the socket loudly
//! with [`WS_MESSAGE_TOO_BIG_CODE`] (1009, Message Too Big) on that failure;
//! the internal presence session ends and reconnects, as on any read error.
//!
//! Before each frame the parser's per-frame limit is narrowed to what the
//! message has left, so an over-cap frame is refused from its header,
//! before its payload is read. The assembly (length and capacity) stays
//! within the cap. The parser's own read buffer is fastwebsockets'
//! (`BytesMut`): it reserves room for each frame, keeps that allocation
//! after handing the payload out, and a later reservation may grow it to
//! about twice the largest frame. A payload is copied out of it before it
//! joins the assembly. Reading one socket's messages therefore holds up to
//! about 32 MiB of assembly plus about 64 MiB in the parser: under 100 MiB,
//! where an uncapped peer could previously grow it without bound.
//!
//! Bytes held mid-assembly are deliberately *not* charged against
//! [`crate::isolate_pipe::ISOLATE_PIPE_MAX_STAGED_BYTES`]: the assembly
//! buffer is bounded by this cap on its own, and it is released into the
//! staged queue — where the staged cap does count it — the moment the
//! message completes. The worst case per socket is therefore the read bound
//! above plus one full staged budget (under 100 MiB + 16 MiB), never an
//! unbounded flood. Counting the same bytes in both ledgers would need a
//! charge transfer at completion and risk the double-release the pipe
//! budgets exist to avoid.

use std::future::Future;

use fastwebsockets::{Frame, OpCode, Payload, WebSocketError, WebSocketRead};

/// Maximum bytes in one assembled WebSocket message: the initial frame plus
/// every continuation, text or binary alike.
///
/// Deliberately Cloudflare's number: Workers fail a WebSocket message over
/// 32 MiB (raised from 1 MiB on 2025-10-31), so a message Cello delivers
/// intact is one Cloudflare would deliver too, and a message Cloudflare
/// refuses never grows a Cello node past this. A single frame is held to
/// the same bound: the parser's per-frame limit is what the message has
/// left.
pub const WS_MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

/// Largest control-frame payload (RFC 6455 §5.5). Control frames interleave
/// a fragmented message and never count toward it, so the per-frame limit
/// never drops below this even when the message has no room left.
const MAX_CONTROL_PAYLOAD: usize = 125;

/// Close code for a message that passed the cap: 1009 (Message Too Big).
/// Application-originated — no peer sent it — so it reports unclean, the way
/// the 1011 overload close does.
pub const WS_MESSAGE_TOO_BIG_CODE: u16 = 1009;

/// Reason for a 1009 close: abnormal, no peer sent it.
pub const WS_MESSAGE_TOO_BIG_REASON: &str = "WebSocket message over its 32 MiB cap";

/// A fragment assembler over [`WebSocketRead::read_frame`] with the total
/// capped at [`WS_MAX_MESSAGE_BYTES`].
///
/// Behaves like `FragmentCollectorRead` — single-frame messages pass
/// through, fragments accumulate until FIN, control frames pass through
/// mid-assembly, automatic ping/pong and close replies still go out through
/// the callback — except the running total is checked on every fragment and
/// the read fails with [`WebSocketError::FrameTooLarge`] once the message
/// passes the cap. The partial assembly is dropped with any error; the
/// caller is expected to tear the socket down, never to keep reading on the
/// desynchronised stream.
///
/// Before each frame the read half's per-frame `max_message_size` is set to
/// what the message has left (at least a control frame's 125 bytes), so a
/// frame that would pass the cap fails from its header without its payload
/// ever being buffered.
pub struct CappedFragmentReader<S> {
    inner: WebSocketRead<S>,
    opcode: Option<OpCode>,
    buf: Vec<u8>,
}

impl<S> CappedFragmentReader<S>
where
    S: tokio::io::AsyncRead + Unpin,
{
    /// Wrap a split read half; the per-frame limit is set before each read.
    pub fn new(reader: WebSocketRead<S>) -> Self {
        Self {
            inner: reader,
            opcode: None,
            buf: Vec::new(),
        }
    }

    /// Bytes held for the message being assembled. A test hook: together
    /// with the per-frame limit it proves the server never buffers more than
    /// the cap.
    pub fn buffered_bytes(&self) -> usize {
        self.buf.len()
    }

    /// Read one message, assembling fragments until the final frame, exactly
    /// like `FragmentCollectorRead::read_frame`.
    pub async fn read_frame<'f, R, E>(
        &mut self,
        send_fn: &mut impl FnMut(Frame<'f>) -> R,
    ) -> Result<Frame<'f>, WebSocketError>
    where
        R: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    {
        loop {
            // fastwebsockets 0.8.1 refuses `payload_len >= max_message_size`,
            // so the limit is one past the largest frame this message can
            // still take: exactly the cap fits, one byte more does not.
            let room = WS_MAX_MESSAGE_BYTES.saturating_sub(self.buf.len());
            self.inner
                .set_max_message_size(room.max(MAX_CONTROL_PAYLOAD) + 1);
            let frame = match self.inner.read_frame(send_fn).await {
                Ok(frame) => frame,
                Err(error) => {
                    self.opcode = None;
                    self.buf = Vec::new();
                    return Err(error);
                }
            };
            match frame.opcode {
                OpCode::Text | OpCode::Binary => {
                    // A new message while one is assembling: the stream is
                    // misbehaving, as with the collector before.
                    if self.opcode.is_some() {
                        self.opcode = None;
                        self.buf = Vec::new();
                        return Err(WebSocketError::InvalidFragment);
                    }
                    // Unreachable past the per-frame limit; kept so the cap
                    // never rests on the parser alone.
                    if frame.payload.len() > WS_MAX_MESSAGE_BYTES {
                        return Err(WebSocketError::FrameTooLarge);
                    }
                    let payload: Vec<u8> = frame.payload.into();
                    if frame.fin {
                        if frame.opcode == OpCode::Text && std::str::from_utf8(&payload).is_err() {
                            return Err(WebSocketError::InvalidUTF8);
                        }
                        return Ok(Frame::new(
                            true,
                            frame.opcode,
                            None,
                            Payload::Owned(payload),
                        ));
                    }
                    self.opcode = Some(frame.opcode);
                    self.buf = payload;
                }
                OpCode::Continuation => {
                    let Some(opcode) = self.opcode else {
                        return Err(WebSocketError::InvalidContinuationFrame);
                    };
                    // Check before appending: the buffer itself never holds
                    // more than the cap. The per-frame limit refuses most
                    // over-cap continuations first; this catches the
                    // control-sized slack the limit keeps.
                    let needed = self.buf.len().saturating_add(frame.payload.len());
                    if needed > WS_MAX_MESSAGE_BYTES {
                        self.opcode = None;
                        self.buf = Vec::new();
                        return Err(WebSocketError::FrameTooLarge);
                    }
                    // Grow geometrically (amortised, so a flood of tiny
                    // fragments stays linear) but never past the cap:
                    // `Vec`'s own doubling could reserve up to twice it.
                    if needed > self.buf.capacity() {
                        let target = needed
                            .max(self.buf.capacity().saturating_mul(2))
                            .min(WS_MAX_MESSAGE_BYTES);
                        self.buf.reserve_exact(target - self.buf.len());
                    }
                    self.buf.extend_from_slice(&frame.payload);
                    if frame.fin {
                        self.opcode = None;
                        let assembled = std::mem::take(&mut self.buf);
                        if opcode == OpCode::Text && std::str::from_utf8(&assembled).is_err() {
                            return Err(WebSocketError::InvalidUTF8);
                        }
                        return Ok(Frame::new(true, opcode, None, Payload::Owned(assembled)));
                    }
                }
                // Control frames interleave fragments on the wire, so they
                // pass through with the assembly kept: the close the peer
                // sends mid-flood still ends the socket as its own close.
                // Copied out: the frame borrows the read half, and the next
                // loop turn needs it back.
                _ => {
                    return Ok(Frame::new(
                        frame.fin,
                        frame.opcode,
                        None,
                        Payload::Owned(frame.payload.into()),
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn client_pair() -> (
        fastwebsockets::WebSocket<tokio::io::DuplexStream>,
        CappedFragmentReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    ) {
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let client =
            fastwebsockets::WebSocket::after_handshake(client_stream, fastwebsockets::Role::Client);
        let (reader, _writer) =
            fastwebsockets::WebSocket::after_handshake(server_stream, fastwebsockets::Role::Server)
                .split(tokio::io::split);
        (client, CappedFragmentReader::new(reader))
    }

    /// The failed read's error, without requiring `Frame: Debug`.
    async fn read_error<S>(reader: &mut CappedFragmentReader<S>) -> WebSocketError
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        let mut send = null_send;
        match reader.read_frame(&mut send).await {
            Ok(_) => panic!("over-cap read must fail"),
            Err(error) => error,
        }
    }

    fn null_send(_: Frame<'_>) -> impl Future<Output = Result<(), std::convert::Infallible>> {
        std::future::ready(Ok(()))
    }

    #[tokio::test]
    async fn unfragmented_message_passes_through() {
        let (mut client, mut reader) = client_pair().await;
        client
            .write_frame(Frame::text(b"hello".to_vec().into()))
            .await
            .expect("client writes");
        let mut send = null_send;
        let frame = reader
            .read_frame(&mut send)
            .await
            .expect("single frame reads");
        assert_eq!(frame.opcode, OpCode::Text);
        assert!(frame.fin);
        assert_eq!(&frame.payload[..], b"hello");
        assert_eq!(reader.buffered_bytes(), 0);
    }

    #[tokio::test]
    async fn fragmented_message_at_exactly_cap_delivered_intact() {
        let (mut client, mut reader) = client_pair().await;
        // 32 × 1 MiB fragments: the total lands exactly on the cap. Writer
        // and reader run joined in this one task: a 1 MiB fragment never
        // fits the 64 KiB duplex buffer whole, so sequential writes would
        // park forever with nobody reading.
        const CHUNK: usize = 1024 * 1024;
        const FRAGMENTS: usize = 32;
        let write_all = async {
            client
                .write_frame(Frame::new(
                    false,
                    OpCode::Binary,
                    None,
                    vec![0xABu8; CHUNK].into(),
                ))
                .await
                .expect("first fragment writes");
            for _ in 1..FRAGMENTS - 1 {
                client
                    .write_frame(Frame::new(
                        false,
                        OpCode::Continuation,
                        None,
                        vec![0xCDu8; CHUNK].into(),
                    ))
                    .await
                    .expect("continuation writes");
            }
            client
                .write_frame(Frame::new(
                    true,
                    OpCode::Continuation,
                    None,
                    vec![0xCDu8; CHUNK].into(),
                ))
                .await
                .expect("final fragment writes");
        };
        // The reader only answers once the final frame lands: the whole
        // 32 MiB comes back, since the cap trips past — not at — the limit.
        let read_all = async {
            let mut send = null_send;
            reader
                .read_frame(&mut send)
                .await
                .expect("message at exactly the cap delivers")
        };
        let (_, frame) = tokio::join!(write_all, read_all);
        assert_eq!(frame.opcode, OpCode::Binary);
        assert!(frame.fin);
        assert_eq!(frame.payload.len(), WS_MAX_MESSAGE_BYTES);
        assert_eq!(&frame.payload[..CHUNK], &vec![0xABu8; CHUNK][..]);
        assert_eq!(&frame.payload[CHUNK..2 * CHUNK], &vec![0xCDu8; CHUNK][..]);
        assert_eq!(reader.buffered_bytes(), 0, "assembly releases on delivery");
    }

    #[tokio::test]
    async fn assembly_past_cap_fails_without_buffering_more() {
        let (mut client, mut reader) = client_pair().await;
        // 33 × 1 MiB fragments with no FIN: the 33rd is refused from its
        // header. Writer and reader run joined in this one task, as above.
        // The refused frame's payload is never read, so the stream is
        // desynchronised past the error: the reader drops its half instead
        // of draining, and the writer stops at the first failed write.
        const CHUNK: usize = 1024 * 1024;
        let write_all = async move {
            client
                .write_frame(Frame::new(
                    false,
                    OpCode::Binary,
                    None,
                    vec![0u8; CHUNK].into(),
                ))
                .await
                .expect("first fragment writes");
            for _ in 1..=32 {
                let continuation =
                    Frame::new(false, OpCode::Continuation, None, vec![0u8; CHUNK].into());
                if client.write_frame(continuation).await.is_err() {
                    return;
                }
            }
        };
        let read_all = async move {
            let error = read_error(&mut reader).await;
            let buffered = reader.buffered_bytes();
            drop(reader);
            (error, buffered)
        };
        let (_, (error, buffered)) = tokio::join!(write_all, read_all);
        assert!(
            matches!(error, WebSocketError::FrameTooLarge),
            "over-cap assembly reports FrameTooLarge: {error:?}"
        );
        assert_eq!(buffered, 0, "a failed read drops the partial assembly");
    }

    /// A server-side pair with the raw client stream, for hand-written
    /// headers that promise more payload than they send.
    fn raw_pair() -> (
        tokio::io::DuplexStream,
        CappedFragmentReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    ) {
        let (client_stream, server_stream) = tokio::io::duplex(64 * 1024);
        let (reader, _writer) =
            fastwebsockets::WebSocket::after_handshake(server_stream, fastwebsockets::Role::Server)
                .split(tokio::io::split);
        (client_stream, CappedFragmentReader::new(reader))
    }

    /// An unmasked frame header (the server role accepts unmasked frames).
    fn raw_header(fin: bool, opcode: u8, len: usize) -> Vec<u8> {
        let mut header = vec![if fin { 0x80 } else { 0 } | opcode];
        if len < 126 {
            header.push(len as u8);
        } else if let Ok(len) = u16::try_from(len) {
            header.push(126);
            header.extend_from_slice(&len.to_be_bytes());
        } else {
            header.push(127);
            header.extend_from_slice(&(len as u64).to_be_bytes());
        }
        header
    }

    #[tokio::test]
    async fn single_frame_of_exactly_cap_delivered() {
        use tokio::io::AsyncWriteExt;
        let (mut raw, mut reader) = raw_pair();
        // fastwebsockets refuses `payload_len >= max_message_size`: a limit
        // of the cap itself would refuse this frame.
        let write_all = async move {
            raw.write_all(&raw_header(true, 0x2, WS_MAX_MESSAGE_BYTES))
                .await
                .expect("header writes");
            raw.write_all(&vec![0x5Au8; WS_MAX_MESSAGE_BYTES])
                .await
                .expect("payload writes");
            raw
        };
        let read_all = async {
            let mut send = null_send;
            reader
                .read_frame(&mut send)
                .await
                .expect("a single frame of exactly the cap delivers")
        };
        let (_raw, frame) = tokio::join!(write_all, read_all);
        assert_eq!(frame.opcode, OpCode::Binary);
        assert_eq!(frame.payload.len(), WS_MAX_MESSAGE_BYTES);
    }

    #[tokio::test]
    async fn single_frame_past_cap_refused_from_header() {
        use tokio::io::AsyncWriteExt;
        let (mut raw, mut reader) = raw_pair();
        raw.write_all(&raw_header(true, 0x2, WS_MAX_MESSAGE_BYTES + 1))
            .await
            .expect("header writes");
        drop(raw);
        // No payload follows: only a refusal from the header answers
        // FrameTooLarge; reading for the payload would hit EOF instead.
        let error = read_error(&mut reader).await;
        assert!(
            matches!(error, WebSocketError::FrameTooLarge),
            "a frame past the cap is refused from its header: {error:?}"
        );
    }

    #[tokio::test]
    async fn continuation_past_room_refused_from_header() {
        use tokio::io::AsyncWriteExt;
        // 100 bytes short of the cap, then a continuation header promising
        // 64 KiB and no payload. With the per-frame limit at the message's
        // room the header alone is refused; a fixed per-frame limit would
        // wait to buffer the 64 KiB and see EOF instead — the in-flight
        // frame would sit beside a near-full assembly.
        const FIRST: usize = WS_MAX_MESSAGE_BYTES - 100;
        let (mut raw, mut reader) = raw_pair();
        let write_all = async move {
            raw.write_all(&raw_header(false, 0x2, FIRST))
                .await
                .expect("header writes");
            raw.write_all(&vec![0u8; FIRST])
                .await
                .expect("payload writes");
            raw.write_all(&raw_header(false, 0x0, 64 * 1024))
                .await
                .expect("continuation header writes");
        };
        let read_all = async {
            let error = read_error(&mut reader).await;
            (error, reader.buffered_bytes())
        };
        let (_, (error, buffered)) = tokio::join!(write_all, read_all);
        assert!(
            matches!(error, WebSocketError::FrameTooLarge),
            "a continuation past the message's room is refused from its header: {error:?}"
        );
        assert_eq!(buffered, 0, "a failed read drops the partial assembly");
    }

    #[tokio::test]
    async fn lone_continuation_still_rejected() {
        let (mut client, mut reader) = client_pair().await;
        client
            .write_frame(Frame::new(
                true,
                OpCode::Continuation,
                None,
                b"orphan".to_vec().into(),
            ))
            .await
            .expect("client writes");
        let error = read_error(&mut reader).await;
        assert!(
            matches!(error, WebSocketError::InvalidContinuationFrame),
            "parity with the old collector: {error:?}"
        );
    }

    #[tokio::test]
    async fn new_message_mid_assembly_still_rejected() {
        let (mut client, mut reader) = client_pair().await;
        client
            .write_frame(Frame::new(
                false,
                OpCode::Text,
                None,
                b"part".to_vec().into(),
            ))
            .await
            .expect("client writes");
        client
            .write_frame(Frame::text(b"other".to_vec().into()))
            .await
            .expect("client writes");
        let error = read_error(&mut reader).await;
        assert!(
            matches!(error, WebSocketError::InvalidFragment),
            "parity with the old collector: {error:?}"
        );
    }
}
