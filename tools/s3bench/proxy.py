#!/usr/bin/env python3
"""Threaded HTTP reverse proxy that logs every S3 request as one JSON line.

Usage: PROXY_PORT=<port> UPSTREAM=<host:port> LOG=<path> [DELAY_MS=<ms>] proxy.py

Each log line is a JSON object with:
  method, path, query, status, bytes_in (request body), bytes_out (response body).

The proxy forwards the Host header exactly as the client sent it, so a
SigV4-signed request still verifies at the upstream: the signature covers
`host`, and path-style addressing needs no host-based routing.
"""

import json
import os
import sys
import threading
import time
from http import client as http_client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = os.environ.get("UPSTREAM", "127.0.0.1:9000")
LOG_PATH = os.environ.get("LOG", "proxy.log")
try:
    DELAY_MS = int(os.environ.get("DELAY_MS", "0"))
except ValueError:
    print("DELAY_MS must be an integer", file=sys.stderr)
    sys.exit(2)

HOST, _, PORT = UPSTREAM.rpartition(":")
UPSTREAM_HOST = HOST or "127.0.0.1"
UPSTREAM_PORT = int(PORT)

HOP_BY_HOP_REQUEST = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}
HOP_BY_HOP_RESPONSE = HOP_BY_HOP_REQUEST | {"content-length"}

_log_lock = threading.Lock()


def log_line(record):
    line = json.dumps(record, separators=(",", ":"))
    with _log_lock:
        with open(LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def read_chunked_body(rfile):
    """Read a chunked request body and return the raw bytes, untouched."""
    raw = bytearray()

    def readline():
        line = bytearray()
        while True:
            byte = rfile.read(1)
            if not byte:
                break
            line += byte
            if line.endswith(b"\r\n"):
                break
        raw.extend(line)
        return bytes(line)

    while True:
        line = readline().strip()
        if not line:
            break
        size = int(line.split(b";")[0], 16)
        if size == 0:
            # Trailing headers, then the final CRLF.
            while True:
                trailer = readline()
                if trailer in (b"\r\n", b""):
                    break
            break
        chunk = rfile.read(size)
        raw.extend(chunk)
        raw.extend(rfile.read(2))  # CRLF after the chunk.
    return bytes(raw)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "s3bench-proxy"

    def log_message(self, *args):  # Silence the default stderr logging.
        pass

    def _proxy(self):
        if DELAY_MS > 0:
            time.sleep(DELAY_MS / 1000.0)
        if self.headers.get("Expect", "").lower() == "100-continue":
            # Answer before reading: the client waits for this, not the body.
            self.wfile.write(b"HTTP/1.1 100 Continue\r\n\r\n")
            self.wfile.flush()
        length = self.headers.get("Content-Length")
        chunked = "chunked" in (self.headers.get("Transfer-Encoding") or "").lower()
        body = b""
        if chunked:
            body = read_chunked_body(self.rfile)
        elif length:
            body = self.rfile.read(int(length))

        upstream = http_client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=120)
        try:
            upstream.putrequest(self.command, self.path, skip_host=True,
                                skip_accept_encoding=True)
            # Forward Host exactly as the client sent it: SigV4 signs `host`,
            # and path-style addressing routes on the path, not the host.
            host = self.headers.get("Host")
            if host is None:
                host = self.headers.get("X-Forwarded-Host", f"{UPSTREAM_HOST}:{UPSTREAM_PORT}")
            upstream.putheader("Host", host)
            for name, value in self.headers.items():
                lowered = name.lower()
                if lowered in ("host", "content-length", "expect") or lowered in HOP_BY_HOP_REQUEST:
                    continue
                upstream.putheader(name, value)
            if chunked:
                upstream.putheader("Transfer-Encoding", "chunked")
            else:
                upstream.putheader("Content-Length", str(len(body)))
            upstream.endheaders()
            if body:
                upstream.send(body)
            response = upstream.getresponse()
            response_body = response.read()
            upstream_length = response.getheader("Content-Length")
        finally:
            upstream.close()

        # Record the upstream result before the downstream write: if the
        # client already went away, the accepted request still counts.
        # Split path and query for the log; keep the raw (still-encoded) path.
        raw_path = self.path
        path, _, query = raw_path.partition("?")
        log_line({
            "method": self.command,
            "path": path,
            "query": query,
            "status": response.status,
            "bytes_in": len(body),
            "bytes_out": 0 if self.command == "HEAD" else len(response_body),
        })

        # HEAD has no body but keeps the object's size metadata.
        if self.command == "HEAD" and upstream_length is not None:
            out_length = upstream_length
        else:
            out_length = str(len(response_body))
        try:
            self.send_response(response.status, response.reason)
            # Upstream's Content-Length is always stripped and exactly one is
            # sent below (HEAD keeps the upstream size via `out_length`).
            for name, value in response.getheaders():
                if name.lower() in HOP_BY_HOP_RESPONSE:
                    continue
                self.send_header(name, value)
            self.send_header("Content-Length", out_length)
            self.send_header("Connection", "close")
            self.end_headers()
            if response_body and self.command != "HEAD":
                self.wfile.write(response_body)
        except (BrokenPipeError, ConnectionResetError):
            # The upstream result is already logged above; a dead client
            # only loses its own response. Stderr lands in proxy.out.
            print(f"forwarding failed: {self.command} {self.path} "
                  f"-> {response.status}", file=sys.stderr)
        self.close_connection = True

    do_GET = _proxy
    do_PUT = _proxy
    do_POST = _proxy
    do_DELETE = _proxy
    do_HEAD = _proxy


def main():
    port = int(os.environ.get("PROXY_PORT", "8080"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    print(f"s3bench proxy: 127.0.0.1:{port} -> {UPSTREAM} (log {LOG_PATH})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
