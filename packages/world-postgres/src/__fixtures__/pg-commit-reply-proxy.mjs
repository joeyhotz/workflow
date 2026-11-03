import { createServer, createConnection } from 'node:net';
import assert from 'node:assert/strict';
import { appendFileSync } from 'node:fs';

// Test-only plaintext PostgreSQL proxy. Backend frames, not TCP chunks, are
// parsed; the selected COMMIT and its following ReadyForQuery never reach pg.
export async function createCommitReplyProxy(upstreamUrl, tracePath) {
  const target = new URL(upstreamUrl);
  assert.ok(['127.0.0.1', 'localhost', '::1'].includes(target.hostname));
  const sockets = new Set();
  const events = [];
  const record = (event) => {
    events.push(event);
    appendFileSync(tracePath, JSON.stringify(event) + '\n');
  };
  let connectionId = 0,
    armed = false,
    cut = false,
    expectedPid,
    activePid;
  const server = createServer((client) => {
    const id = ++connectionId;
    const backend = createConnection({
      host: target.hostname,
      port: Number(target.port || 5432),
    });
    sockets.add(client);
    sockets.add(backend);
    client.once('close', () => {
      sockets.delete(client);
      backend.destroy();
    });
    backend.once('close', () => {
      sockets.delete(backend);
      if (!client.destroyed) client.destroy();
    });
    client.on('error', (error) =>
      record({ id, direction: 'client', error: error.message })
    );
    backend.on('error', (error) =>
      record({ id, direction: 'backend', error: error.message })
    );
    client.pipe(backend);
    let buffered = Buffer.alloc(0),
      suppress = false;
    backend.on('data', (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 5) {
        const size = buffered.readInt32BE(1);
        if (size < 4 || size > 64 * 1024 * 1024) {
          record({ id, error: 'invalid backend frame length', size });
          client.destroy();
          backend.destroy();
          return;
        }
        const length = 1 + size;
        if (buffered.length < length) break;
        const frame = buffered.subarray(0, length);
        buffered = buffered.subarray(length);
        const type = String.fromCharCode(frame[0]);
        if (type === 'K') {
          activePid = frame.readInt32BE(5);
          record({ id, type, pid: activePid });
        }
        const tag =
          type === 'C'
            ? frame.subarray(5, frame.length - 1).toString('utf8')
            : undefined;
        if (armed && !cut && type === 'C' && tag === 'COMMIT') {
          if (activePid !== expectedPid) {
            record({
              id,
              error: 'unexpected backend pid',
              activePid,
              expectedPid,
            });
            client.destroy();
            backend.destroy();
            return;
          }
          suppress = true;
          cut = true;
          armed = false;
          record({ id, type, tag, pid: activePid, suppressed: true });
          continue;
        }
        if (suppress) {
          record({ id, type, tag, suppressed: true });
          if (type === 'Z') {
            client.destroy();
            backend.destroy();
            return;
          }
          continue;
        }
        if (armed && type === 'C') record({ id, type, tag, suppressed: false });
        client.write(frame);
      }
    });
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const proxyUrl = new URL(upstreamUrl);
  proxyUrl.hostname = '127.0.0.1';
  proxyUrl.port = String(server.address().port);
  proxyUrl.searchParams.set('sslmode', 'disable');
  return {
    url: proxyUrl.toString(),
    arm(pid) {
      assert.equal(cut, false);
      assert.equal(armed, false);
      assert.equal(activePid, pid);
      expectedPid = pid;
      armed = true;
      record({ action: 'armed', pid });
    },
    snapshot() {
      return { events: [...events], cut, activePid, expectedPid, connectionId };
    },
    async close() {
      for (const s of sockets) s.destroy();
      await new Promise((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve()))
      );
    },
  };
}
