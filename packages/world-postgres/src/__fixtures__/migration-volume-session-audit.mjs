// Test-only read-only observation of the actual CLI pool checkout.
import { appendFileSync } from 'node:fs';
import pg from 'pg';

const original = pg.Pool.prototype.connect;
async function inspect(client) {
  const result = await client.query(`SELECT pg_backend_pid() pid,
    current_setting('application_name') application_name,
    current_setting('TimeZone') timezone,
    current_setting('workflow.legacy_timezone', true) legacy_timezone,
    current_setting('lock_timeout') lock_timeout,
    current_setting('statement_timeout') statement_timeout`);
  appendFileSync(
    process.env.SLICE5_SESSION_AUDIT,
    JSON.stringify(result.rows[0]) + '\n'
  );
}
pg.Pool.prototype.connect = function (callback) {
  if (typeof callback === 'function') {
    return original.call(this, (error, client, release) => {
      if (error) return callback(error, client, release);
      inspect(client).then(
        () => callback(null, client, release),
        (failure) => {
          release(failure);
          callback(failure);
        }
      );
    });
  }
  return original.call(this).then(async (client) => {
    try {
      await inspect(client);
      return client;
    } catch (error) {
      client.release(error);
      throw error;
    }
  });
};
