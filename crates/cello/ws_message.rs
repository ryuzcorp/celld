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
//! [`WS_MAX_MESSAGE_BYTES`]. The caller ends the socket loudly with
//! [`WS_MESSAGE_TOO_BIG_CODE`] (1009, Message Too Big) on that failure.
//!
//! Bytes held mid-assembly are deliberately *not* charged against
//! [`crate::isolate_pipe::ISOLATE_PIPE_MAX_STAGED_BYTES`]: the assembly
//! buffer is bounded by this cap on its own, and it is released into the
//! staged queue — where the staged cap does count it — the moment the
//! message completes. The worst case per socket is therefore the message cap
//! plus one full staged budget (32 MiB + 16 MiB), never an unbounded flood.
//! Counting the same bytes in both ledgers would need a charge transfer at
//! completion and risk the double-release the pipe budgets exist to avoid.

use std::future::Future;

use fastwebsockets::{Frame, OpCode, Payload, WebSocketError, WebSocketRead};

/// Maximum bytes in one assembled WebSocket message: the initial frame plus
/// every continuation, text or binary alike.
///
/// Deliberately Cloudflare's number: Workers fail a WebSocket message over
/// 32 MiB (raised from 1 MiB on 2025-10-31), so a message Cello delivers
/// intact is one Cloudflare would deliver too, and a message Cloudflare
/// refuses never grows a Cello node past this. The per-frame
/// `max_message_size` is set to the same value when a reader is built, so no
/// single frame can exceed the message it belongs to.
pub const WS_MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

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
/// passes the cap. The partial assembly is dropped with the error; the
/// caller is expected to tear the socket down (loudly, with 1009), never to
/// keep reading on the desynchronised stream.
///
/// The constructor pins the read half's per-frame `max_message_size` to the
/// message cap, so a single frame can never exceed the message it belongs
/// to: a frame at or past the cap fails the same way an over-cap assembly
/// does.
pub struct CappedFragmentReader<S> {
    inner: WebSocketRead<S>,
    opcode: Option<OpCode>,
    buf: Vec<u8>,
}

impl<S> CappedFragmentReader<S>
where
    S: tokio::io::AsyncRead + Unpin,
{
    /// Wrap a split read half, capping its per-frame size at the message cap.
    pub fn new(mut reader: WebSocketRead<S>) -> Self {
        reader.set_max_message_size(WS_MAX_MESSAGE_BYTES);
        Self {
            inner: reader,
            opcode: None,
            buf: Vec::new(),
        }
    }

    /// Bytes held for the message being assembled. A test hook: together
    /// with the per-frame bound it proves the server never buffers more than
    /// the cap plus one frame.
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
            let frame = self.inner.read_frame(send_fn).await?;
            match frame.opcode {
                OpCode::Text | OpCode::Binary => {
                    // A new message while one is assembling: the stream is
                    // misbehaving, as with the collector before.
                    if self.opcode.is_some() {
                        return Err(WebSocketError::InvalidFragment);
                    }
                    let payload: Vec<u8> = frame.payload.into();
                    if payload.len() > WS_MAX_MESSAGE_BYTES {
                        return Err(WebSocketError::FrameTooLarge);
                    }
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
                    // more than the cap, so the peak is bounded even while a
                    // flood is in flight.
                    if self.buf.len().saturating_add(frame.payload.len()) > WS_MAX_MESSAGE_BYTES {
                        self.opcode = None;
                        self.buf.clear();
                        return Err(WebSocketError::FrameTooLarge);
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
        // 32 × 1 MiB fragments: the total lands exactly on the cap. The
        // writer runs alongside the read: a 1 MiB fragment never fits the
        // 64 KiB duplex buffer whole, so sequential writes would park
        // forever with nobody reading.
        const CHUNK: usize = 1024 * 1024;
        const FRAGMENTS: usize = 32;
        let writer = tokio::spawn(async move {
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
        });
        // The reader only answers once the final frame lands: the whole
        // 32 MiB comes back, since the cap trips past — not at — the limit.
        let mut send = null_send;
        let frame = reader
            .read_frame(&mut send)
            .await
            .expect("message at exactly the cap delivers");
        writer.await.expect("sender joins");
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
        // 33 × 1 MiB fragments with no FIN: the 33rd byte past the cap
        // fails the read. The writer runs alongside, as above; it parks
        // once the failed read stops consuming, and is left parked.
        const CHUNK: usize = 1024 * 1024;
        let writer = tokio::spawn(async move {
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
                if client
                    .write_frame(Frame::new(
                        false,
                        OpCode::Continuation,
                        None,
                        vec![0u8; CHUNK].into(),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let error = read_error(&mut reader).await;
        writer.abort();
        assert!(
            matches!(error, WebSocketError::FrameTooLarge),
            "over-cap assembly reports FrameTooLarge: {error:?}"
        );
        assert!(
            reader.buffered_bytes() <= WS_MAX_MESSAGE_BYTES,
            "the buffer itself never holds more than the cap"
        );
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
