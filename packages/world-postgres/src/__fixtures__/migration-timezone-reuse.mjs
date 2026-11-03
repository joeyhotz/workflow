import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const req = createRequire(
  process.env.WORKFLOW_TZ_PULSIFY + '/apps/api/package.json'
);
const worldPath = req.resolve('@workflow/world-postgres');
const { createWorld } = await import(pathToFileURL(worldPath));
const { Pool } = req('pg');
const pool = new Pool({
  connectionString: process.env.WORKFLOW_POSTGRES_URL,
  max: 1,
});
const world = createWorld({
  pool,
  jobPrefix: 'slice4b_',
  recoverActiveRuns: false,
  applicationManagedShutdown: true,
});
const id = 'wrun_01M6GG9R8TXXGXMMQKC6MV4YCY';
const query = {
  name: 'slice4b_timestamp_before',
  text: 'SELECT created_at FROM workflow.workflow_runs WHERE id=$1',
  values: [id],
};
try {
  const before = await world.runs.get(id);
  const { rows: settings } = await pool.query(
    "SELECT pg_backend_pid() pid, current_setting('TimeZone') timezone"
  );
  await pool.query(query);
  await promisify(execFile)(
    process.execPath,
    [process.env.WORKFLOW_TZ_SOURCE + '/packages/world-postgres/bin/setup.js'],
    {
      env: {
        ...process.env,
        PGOPTIONS: '-c TimeZone=UTC -c workflow.legacy_timezone=UTC',
      },
    }
  );
  let named;
  try {
    await pool.query(query);
    named = { ok: true };
  } catch (error) {
    named = { ok: false, code: error.code, message: error.message };
  }
  let sdk;
  try {
    const after = await world.runs.get(id);
    sdk = {
      ok: true,
      createdAt: after.createdAt,
      completedAt: after.completedAt,
    };
  } catch (error) {
    sdk = {
      ok: false,
      message: error.message,
      code: error.code,
      cause: error.cause?.message,
    };
  }
  console.log(
    JSON.stringify({
      exit: 0,
      settings: settings[0],
      worldPath,
      before: { createdAt: before.createdAt, completedAt: before.completedAt },
      named,
      sdk,
    })
  );
} finally {
  await world.close();
  await pool.end();
}
