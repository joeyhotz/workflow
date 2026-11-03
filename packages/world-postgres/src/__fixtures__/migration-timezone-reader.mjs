import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
const old = process.env.WORKFLOW_TZ_LANE === 'old';
const root = old
  ? process.env.WORKFLOW_TZ_PULSIFY + '/apps/api'
  : process.env.WORKFLOW_TZ_TARGET;
const req = createRequire(root + '/package.json');
const apiPath = req.resolve('workflow/api');
const corePath = createRequire(apiPath).resolve('@workflow/core/runtime');
const worldPath = old
  ? req.resolve('@workflow/world-postgres')
  : process.env.WORKFLOW_TZ_SOURCE + '/packages/world-postgres/dist/index.js';
const { createWorld } = await import(pathToFileURL(worldPath));
const coreWorld = old ? join(dirname(corePath), 'runtime/world.js') : corePath;
const { setWorld } = await import(pathToFileURL(coreWorld));
const { getRun } = await import(pathToFileURL(apiPath));
const { Pool } = createRequire(
  process.env.WORKFLOW_TZ_SOURCE + '/packages/world-postgres/package.json'
)('pg');
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
setWorld(world);
try {
  const runId = 'wrun_01M6GG9R8TXXGXMMQKC6MV4YCY';
  const sleepId = 'wrun_01M6GG9R9E9HW687TZKQNHKE3Y';
  const run = getRun(runId);
  const stored = await world.runs.get(runId);
  const steps = await world.steps.list({ runId, pagination: { limit: 100 } });
  const events = await world.events.list({
    runId: sleepId,
    pagination: { limit: 100 },
  });
  const settings = (
    await pool.query(
      "SELECT pg_backend_pid() pid,current_setting('TimeZone') timezone,current_setting('search_path') search_path"
    )
  ).rows[0];
  let prepared;
  if (!old) {
    await pool.query({
      name: 'slice4b_target_plan',
      text: 'SELECT created_at FROM workflow.workflow_runs WHERE id=$1',
      values: [runId],
    });
    prepared = (
      await pool.query(
        "SELECT name FROM pg_prepared_statements WHERE name='slice4b_target_plan'"
      )
    ).rows;
  }
  console.log(
    JSON.stringify({
      exit: 0,
      settings,
      prepared,
      nodeTimezone: process.env.TZ,
      pgOptions: process.env.PGOPTIONS,
      apiPath,
      corePath,
      worldPath,
      createdAt: stored.createdAt,
      completedAt: await run.completedAt,
      status: await run.status,
      steps: steps.data.map((s) => ({
        id: s.stepId,
        createdAt: s.createdAt,
        completedAt: s.completedAt,
      })),
      events: events.data.map((e) => ({
        id: e.eventId,
        type: e.eventType,
        createdAt: e.createdAt,
        eventData: e.eventData,
      })),
    })
  );
} finally {
  await world.close();
  await pool.end();
}
