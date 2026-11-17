// Modified by ryuzcorp (ryuzcorp/celld): full-disk recovery fixture (upstream #291).
import { DurableObject } from 'cloudflare:workers';

// A cell that writes blobs in a transaction and reads them back. Clamping
// the node's file-size limit makes the transactional write fail with a
// storage I/O error, which destroys the transaction exactly like the
// SQLITE_FULL of a full disk; every later request — reads included —
// must not wedge behind that failure (upstream #291).
export class Cell extends DurableObject {
  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === '/write') {
      const n = Number(url.searchParams.get('n') || '1');
      const blob = 'x'.repeat(256 * 1024);
      this.ctx.storage.transactionSync(() => {
        this.ctx.storage.sql.exec(
          'CREATE TABLE IF NOT EXISTS t(v TEXT)',
        );
        for (let i = 0; i < n; i++) {
          this.ctx.storage.sql.exec('INSERT INTO t(v) VALUES (?)', blob);
        }
      });
      return new Response('wrote');
    }
    if (url.pathname === '/read') {
      const rows = [...this.ctx.storage.sql.exec('SELECT COUNT(*) AS n FROM t')];
      return new Response(`rows=${rows[0].n}`);
    }
    return new Response('ok');
  }
}

export default {
  fetch(req, env) {
    return env.CELL.get(env.CELL.idFromName('one')).fetch(req);
  },
};
