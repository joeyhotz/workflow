import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { readFile, writeFile } from 'node:fs/promises';
const base = process.env.WORKFLOW_SLICE4A_PULSIFY;
const req = createRequire(base + '/apps/api/package.json');
const runtimePath = req.resolve('workflow/runtime');
const corePath = createRequire(runtimePath).resolve('@workflow/core/runtime');
const dir = path.dirname(corePath);
const worldPath = req.resolve('@workflow/world-postgres');
const { Pool } = req('pg');
const { createWorld } = await import(pathToFileURL(worldPath));
const { setWorld } = await import(
  pathToFileURL(path.join(dir, 'runtime/world.js'))
);
const { registerStepFunction } = await import(
  pathToFileURL(path.join(dir, 'private.js'))
);
const { start } = await import(
  pathToFileURL(path.join(dir, 'runtime/start.js'))
);
const api = await import(pathToFileURL(req.resolve('workflow/api')));
const runtime = await import(pathToFileURL(runtimePath));
const { RetryableError } = await import(
  pathToFileURL(createRequire(corePath).resolve('@workflow/errors'))
);
const { readDrainSnapshot } = await import(
  pathToFileURL(base + '/apps/api/scripts/workflow-major-cutover.ts')
);
const url = process.env.WORKFLOW_SLICE4A_URL;
const pool = new Pool({ connectionString: url });
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
const phase = process.env.WORKFLOW_SLICE4A_PHASE;
assert.ok(phase === 'start' || phase === 'finish');
const out = {
  pid: process.pid,
  phase,
  exit: 1,
  stage: 'init',
  identities: { runtimePath, corePath, worldPath },
};
let world;
let server;
try {
  if (phase === 'start')
    await pool.query(
      'CREATE TABLE public.slice4a_effects (label text PRIMARY KEY, count integer NOT NULL)'
    );
  const bump = async (label) =>
    (
      await pool.query(
        'INSERT INTO public.slice4a_effects VALUES ($1,1) ON CONFLICT(label) DO UPDATE SET count=public.slice4a_effects.count+1 RETURNING count',
        [label]
      )
    ).rows[0].count;
  const code = `globalThis.__private_workflows=new Map();const step=globalThis[Symbol.for('WORKFLOW_USE_STEP')];const hook=globalThis[Symbol.for('WORKFLOW_CREATE_HOOK')];const sleep=globalThis[Symbol.for('WORKFLOW_SLEEP')];const business=step('step//slice4a//business');const spawn=step('step//slice4a//spawn');const retry=step('step//slice4a//retry');
 globalThis.__private_workflows.set('workflow//slice4a//root',async()=>{const value=await business('root');const child=await spawn();const h=hook({token:'slice4a-root'});const msg=await h;h.dispose();return {value,child,msg}});
 globalThis.__private_workflows.set('workflow//slice4a//child',async()=>{const h=hook({token:'slice4a-child'});const msg=await h;h.dispose();return {child:msg}});
 globalThis.__private_workflows.set('workflow//slice4a//sleep',async()=>{await sleep('30s');return 'slept'});
 globalThis.__private_workflows.set('workflow//slice4a//retry',async()=>await retry());
 globalThis.__private_workflows.set('workflow//slice4a//failure',async()=>{await business('failure');throw new Error('slice4a legacy failure',{cause:new Error('slice4a nested')})});`;
  let flow = runtime.workflowEntrypoint(code);
  const step = runtime.stepEntrypoint;
  server = createServer(async (req, res) => {
    try {
      let chunks = [];
      for await (const c of req) chunks.push(c);
      const r = new Request('http://127.0.0.1' + req.url, {
        method: req.method,
        headers: req.headers,
        body: Buffer.concat(chunks),
      });
      const answer = await (req.url?.includes('/step') ? step : flow)(r);
      res.writeHead(answer.status, Object.fromEntries(answer.headers));
      res.end(Buffer.from(await answer.arrayBuffer()));
    } catch (e) {
      console.error('handler', e);
      res.writeHead(500);
      res.end('fixture error');
    }
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  process.env.WORKFLOW_LOCAL_BASE_URL = `http://127.0.0.1:${server.address().port}`;
  const newWorld = () =>
    createWorld({
      connectionString: url,
      jobPrefix: 'api_',
      queueConcurrency: 5,
      recoverActiveRuns: false,
      applicationManagedShutdown: true,
    });
  world = newWorld();
  setWorld(world);
  const workflow = (name) => ({ workflowId: `workflow//slice4a//${name}` });
  registerStepFunction('step//slice4a//business', async (label) => ({
    label,
    count: await bump(label),
  }));
  registerStepFunction('step//slice4a//spawn', async () => {
    const child = await start(workflow('child'), [], { world });
    return child.runId;
  });
  registerStepFunction('step//slice4a//retry', async () => {
    const attempt = await bump('retry');
    if (attempt === 1)
      throw new RetryableError('slice4a retry', { retryAfter: '30s' });
    return { attempt };
  });
  if (phase === 'start') await world.start();
  let ids;
  if (phase === 'start') {
    const root = await start(workflow('root'), [], { world });
    const sleepy = await start(workflow('sleep'), [], { world });
    const retryRun = await start(workflow('retry'), [], { world });
    const failure = await start(workflow('failure'), [], { world });
    let childId;
    out.stage = 'await-partial';
    for (let i = 0; i < 150; i++) {
      const q = await pool.query(
        "SELECT run_id,token FROM workflow.workflow_hooks WHERE token IN ('slice4a-root','slice4a-child') ORDER BY token"
      );
      const waits = await pool.query(
        "SELECT count(*)::int n FROM workflow.workflow_waits WHERE status='waiting' AND run_id=$1",
        [sleepy.runId]
      );
      const effect = await pool.query(
        "SELECT count FROM public.slice4a_effects WHERE label='root'"
      );
      const retry = await pool.query(
        "SELECT count(*)::int n FROM workflow.workflow_steps WHERE retry_after > now() AND status='pending' AND run_id=$1",
        [retryRun.runId]
      );
      if (
        q.rows.length === 2 &&
        waits.rows[0].n === 1 &&
        retry.rows[0].n === 1 &&
        effect.rowCount === 1 &&
        effect.rows[0].count === 1
      ) {
        childId = q.rows.find((x) => x.token === 'slice4a-child').run_id;
        out.partial = {
          hooks: q.rows,
          sleepWaits: waits.rows[0].n,
          retryPending: retry.rows[0].n,
          rootEffect: effect.rows[0].count,
        };
        break;
      }
      await pause(100);
    }
    assert.ok(
      childId,
      'partial root/child/hook/sleep/retry did not materialize'
    );
    ids = {
      root: root.runId,
      child: childId,
      sleep: sleepy.runId,
      retry: retryRun.runId,
      failure: failure.runId,
    };
    out.ids = ids;
    out.stage = 'partial';
    out.exit = 0;
  } else {
    const partial = JSON.parse(
      await readFile(
        process.env.WORKFLOW_SLICE4A_RECEIPT + '/partial.json',
        'utf8'
      )
    );
    const bridge = JSON.parse(
      await readFile(
        process.env.WORKFLOW_SLICE4A_RECEIPT + '/bridge.json',
        'utf8'
      )
    );
    assert.notEqual(process.pid, partial.pid);
    ids = partial.ids;
    out.ids = ids;
    out.partial = partial.partial;
    out.drain = bridge.drain;
    const ready = await pool.query(
      "SELECT (SELECT count(*)::int FROM workflow.workflow_waits WHERE run_id=$1 AND status='waiting' AND resume_at>now()) AS sleeps, (SELECT count(*)::int FROM workflow.workflow_steps WHERE run_id=$2 AND status='pending' AND retry_after>now()) AS retries, (SELECT count FROM public.slice4a_effects WHERE label='root') AS root_effect",
      [ids.sleep, ids.retry]
    );
    assert.deepEqual(ready.rows[0], { sleeps: 1, retries: 1, root_effect: 1 });
    out.restart = {
      oldPid: partial.pid,
      newPid: process.pid,
      beforeStart: ready.rows[0],
    };
    await world.start();
    out.stage = 'restart-v4';
    await api.resumeHook('slice4a-child', { accepted: 'child' });
    await api.resumeHook('slice4a-root', { accepted: 'root' });
    for (let i = 0; i < 500; i++) {
      const r = await pool.query(
        'SELECT id,status::text FROM workflow.workflow_runs WHERE id=ANY($1::text[])',
        [Object.values(ids)]
      );
      if (
        r.rows.length === 5 &&
        r.rows.every((x) => ['completed', 'failed'].includes(x.status))
      )
        break;
      if (i === 499)
        throw new Error('terminal timeout ' + JSON.stringify(r.rows));
      await pause(100);
    }
    for (let i = 0; i < 100; i++) {
      const c = await readDrainSnapshot(pool);
      if (c.active === 0 && c.legacyJobs === 0 && c.newJobs === 0) {
        out.finalCounts = c;
        break;
      }
      await pause(100);
    }
    assert.ok(out.finalCounts, 'jobs did not drain naturally');
    const effects = await pool.query(
      'SELECT label,count FROM public.slice4a_effects ORDER BY label'
    );
    assert.deepEqual(effects.rows, [
      { label: 'failure', count: 1 },
      { label: 'retry', count: 2 },
      { label: 'root', count: 1 },
    ]);
    out.effects = effects.rows;
    out.stage = 'old-read';
    const data = {};
    for (const [name, id] of Object.entries(ids)) {
      const run = api.getRun(id);
      const status = await run.status;
      let result, error;
      try {
        result = await run.returnValue;
      } catch (e) {
        error = {
          name: e.name,
          message: e.message,
          cause: e.cause
            ? {
                message: e.cause.message,
                code: e.cause.code,
                stack: e.cause.stack,
              }
            : null,
        };
      }
      const events = await world.events.list({
        runId: id,
        pagination: { limit: 100, sortOrder: 'asc' },
      });
      const steps = await world.steps.list({
        runId: id,
        pagination: { limit: 100, sortOrder: 'asc' },
      });
      const hooks = await world.hooks.list({
        runId: id,
        pagination: { limit: 100, sortOrder: 'asc' },
      });
      data[name] = {
        status,
        completedAt: await run.completedAt,
        result,
        error,
        events: events.data.map((e) => ({
          eventId: e.eventId,
          eventType: e.eventType,
          correlationId: e.correlationId,
        })),
        steps: steps.data.map((s) => ({ stepId: s.stepId, status: s.status })),
        hooks: hooks.data,
      };
    }
    assert.equal(data.failure.status, 'failed');
    assert.equal(data.failure.error.cause.message, 'slice4a legacy failure');
    assert.equal(data.root.result.value.count, 1);
    out.data = data;
    const rows = await pool.query(
      'SELECT hash,created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
    );
    assert.equal(rows.rowCount, 12);
    out.journal = rows.rows;
    out.stage = 'complete';
    out.exit = 0;
  }
} catch (e) {
  out.error = { message: e.message, stack: e.stack };
  console.error(e);
} finally {
  if (world) await world.close();
  if (server) await new Promise((r) => server.close(r));
  await pool.end();
  await writeFile(
    process.env.WORKFLOW_SLICE4A_RECEIPT +
      (phase === 'start' ? '/partial.json' : '/old.json'),
    JSON.stringify(out, null, 2)
  );
}
if (out.exit !== 0) process.exitCode = 1;
