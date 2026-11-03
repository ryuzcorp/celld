// Modified by ryuzcorp (ryuzcorp/celld): tenant egress policy fixture. Apache-2.0.
import { connect } from 'cloudflare:sockets';

const J = (o, st = 200) => Response.json(o, { status: st });
// A port nothing listens on: refusals happen before any dial, so the
// value only matters for allowlisted-port assertions.
const CLOSED = 9;

const refused = async (fn) => {
  try {
    await fn();
    return { refused: false };
  } catch (e) {
    return { refused: true, name: e?.name ?? typeof e, message: e?.message ?? String(e) };
  }
};

export default {
  async fetch(req, env) {
    const t = new URL(req.url).pathname.slice(1);
    const echo = Number(env.ECHO_PORT);
    const run = async () => {
      if (t === 'health') return { ok: true };
      switch (t) {
        case 'fetch-loopback':
          return await refused(() => fetch(`http://127.0.0.1:${CLOSED}/`));
        case 'fetch-private':
          return await refused(() => fetch('http://10.255.255.1/'));
        case 'fetch-metadata':
          return await refused(() => fetch('http://169.254.169.254/'));
        case 'fetch-cgnat':
          return await refused(() => fetch('http://100.64.0.1/'));
        case 'fetch-v6-loopback':
          return await refused(() => fetch(`http://[::1]:${CLOSED}/`));
        case 'fetch-mapped':
          return await refused(() => fetch(`http://[::ffff:127.0.0.1]:${CLOSED}/`));
        case 'fetch-localhost':
          return await refused(() => fetch(`http://localhost:${CLOSED}/`));
        case 'fetch-allowlisted': {
          const res = await fetch(`http://127.0.0.1:${echo}/echo`);
          return { status: res.status, body: await res.text() };
        }
        case 'fetch-allowlisted-wrongport':
          // The allowlist covers the echo port only: any other loopback
          // port stays refused.
          return await refused(() => fetch(`http://127.0.0.1:${echo + 1}/`));
        case 'fetch-redirect-private':
          // The echo server answers a redirect to a non-allowlisted
          // loopback port; the hop must re-check.
          return await refused(() => fetch(`http://127.0.0.1:${echo}/redir`));
        case 'fetch-public-unreachable': {
          // TEST-NET-1 never routes: the policy passes it and the dial
          // fails, bounded by the node's short fetch timeout.
          const outcome = await refused(() => fetch('http://203.0.113.1/'));
          return { ...outcome, policed: (outcome.message ?? '').includes('egress policy') };
        }
        case 'connect-private': {
          const sock = connect('10.255.255.1:80');
          return await refused(() => sock.opened);
        }
        case 'connect-allowlisted': {
          const sock = connect({ hostname: '127.0.0.1', port: echo });
          const info = await sock.opened;
          const writer = sock.writable.getWriter();
          await writer.write(new TextEncoder().encode('ping'));
          writer.releaseLock();
          const reader = sock.readable.getReader();
          const { value } = await reader.read();
          reader.releaseLock();
          await sock.close();
          return { remote: info.remoteAddress, echo: new TextDecoder().decode(value) };
        }
        case 'ws-private': {
          const ws = new WebSocket('ws://10.255.255.1:80/');
          return await new Promise((resolve) => {
            ws.addEventListener('error', (e) =>
              resolve({ refused: true, message: e?.message ?? String(e) }));
            ws.addEventListener('open', () => resolve({ refused: false }));
          });
        }
        case 'ws-allowlisted': {
          const ws = new WebSocket(`ws://127.0.0.1:${echo}/ws`);
          return await new Promise((resolve, reject) => {
            ws.addEventListener('open', () => ws.send('hello-ws'));
            ws.addEventListener('message', (e) => {
              ws.close();
              resolve({ echo: typeof e.data === 'string' ? e.data : 'binary' });
            });
            ws.addEventListener('error', (e) =>
              reject(new Error(`ws-allowlisted errored: ${e?.message ?? e}`)));
          });
        }
        default: return { error: `unknown ${t}` };
      }
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
};
