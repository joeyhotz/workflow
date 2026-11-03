import assert from 'node:assert/strict';
import { createRequire, registerHooks } from 'node:module';
import { createServer } from 'node:http';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { dirname, join } from 'node:path';
import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
const loaded = new Set();
const hook = registerHooks({
  load(url, context, next) {
    if (url.startsWith('file:')) loaded.add(fileURLToPath(url));
    return next(url, context);
  },
});
const req = createRequire(process.env.SLICE4C3_TARGET + '/package.json');
const runtimePath = req.resolve('workflow/runtime');
const corePath = createRequire(runtimePath).resolve('@workflow/core/runtime');
const coreDir = dirname(corePath);
const worldPath =
  process.env.SLICE4C3_SOURCE + '/packages/world-postgres/dist/index.js';
const { Pool } = createRequire(
  process.env.SLICE4C3_SOURCE + '/packages/world-postgres/package.json'
)('pg');
const { createWorld } = await import(pathToFileURL(worldPath));
const { createEventsStorage } = await import(
  pathToFileURL(
    process.env.SLICE4C3_SOURCE + '/packages/world-postgres/dist/storage.js'
  )
);
const { createClient } = await import(
  pathToFileURL(
    process.env.SLICE4C3_SOURCE +
      '/packages/world-postgres/dist/drizzle/index.js'
  )
);
const { createCommitReplyProxy } = await import('./pg-commit-reply-proxy.mjs');
const { handleConnectionErrors } = await import(
  pathToFileURL(
    process.env.SLICE4C3_SOURCE +
      '/packages/world-postgres/dist/connection-errors.js'
  )
);
const { setWorld } = await import(pathToFileURL(corePath));
const runtime = await import(pathToFileURL(runtimePath));
const { registerStepFunction } = await import(
  pathToFileURL(join(coreDir, 'private.js'))
);
const { start } = await import(
  pathToFileURL(join(coreDir, 'runtime/start.js'))
);
const api = await import(pathToFileURL(req.resolve('workflow/api')));
const { FatalError, EntityConflictError } = await import(
  pathToFileURL(createRequire(corePath).resolve('@workflow/errors'))
);
const constants = await import(
  pathToFileURL(join(coreDir, 'runtime/constants.js'))
);
const pool = new Pool({ connectionString: process.env.SLICE4C3_URL });
const observer = new Pool({ connectionString: process.env.SLICE4C3_URL });
const shape = process.env.SLICE4C3_SHAPE,
  outcome = process.env.SLICE4C3_OUTCOME,
  mode = 'commit-response-loss';
const out = {
  exit: 1,
  stage: 'init',
  shape,
  outcome,
  mode,
  pid: process.pid,
  paths: { runtimePath, corePath, worldPath },
  deliveries: [],
  injections: 0,
  conflicts: [],
  settings: {
    spec: process.env.WORKFLOW_SPEC_VERSION,
    trace: process.env.WORKFLOW_TRACE_MODE,
    earlyAdvance: process.env.WORKFLOW_INLINE_EARLY_ADVANCE,
    invoke: process.env.WORKFLOW_POSTGRES_INVOKE,
    startHook: process.env.WORKFLOW_POSTGRES_EXPERIMENTAL_START_HOOK,
    skipRecovery: process.env.WORKFLOW_SKIP_BOOT_RECOVERY,
    jobPrefix: process.env.WORKFLOW_POSTGRES_JOB_PREFIX,
    inlineLimit: constants.getMaxInlineSteps(),
  },
};
let world,
  server,
  proxy,
  proxyPool,
  proxyStorage,
  proxyPid,
  runId,
  initialFlowSettled = false;
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
const bump = async (label) =>
  (
    await pool.query(
      'INSERT INTO public.slice4c3_effects VALUES ($1,1) ON CONFLICT(label) DO UPDATE SET count=public.slice4c3_effects.count+1 RETURNING count',
      [label]
    )
  ).rows[0].count;
try {
  assert.equal(out.settings.inlineLimit, 3);
  const code = `globalThis.__private_workflows=new Map();const target=globalThis[Symbol.for('WORKFLOW_USE_STEP')]('step//slice4c3//target');const sibling=globalThis[Symbol.for('WORKFLOW_USE_STEP')]('step//slice4c3//sibling');const continuation=globalThis[Symbol.for('WORKFLOW_USE_STEP')]('step//slice4c3//continuation');globalThis.__private_workflows.set('workflow//slice4c3//main',async()=>{let observed;try{${shape === 'queued' ? "const values=await Promise.all([sibling('s1'),sibling('s2'),sibling('s3'),target()]);observed={kind:'success',value:values[3]};" : "observed={kind:'success',value:await target()};"}}catch(e){observed={kind:'failure',name:e.name,message:e.message,cause:e.cause?{name:e.cause.name,message:e.cause.message}:null}}const next=await continuation(observed);return {observed,next}});`;
  const flow = runtime.workflowEntrypoint(code, { namespace: 'slice4c3' });
  server = createServer(async (req, res) => {
    const entry = { path: req.url, headers: req.headers };
    out.deliveries.push(entry);
    try {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const bytes = Buffer.concat(chunks);
      entry.body = JSON.parse(bytes.toString());
      entry.job = (
        await observer.query(
          `SELECT j.id::text,t.identifier,j.attempts,j.locked_by FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id WHERE j.payload->>'messageId'=$1`,
          [req.headers['x-vqs-message-id']]
        )
      ).rows[0];
      const request = new Request('http://127.0.0.1' + req.url, {
        method: req.method,
        headers: req.headers,
        body: bytes,
      });
      const response = await flow(request);
      entry.status = response.status;
      res.writeHead(response.status, Object.fromEntries(response.headers));
      res.end(Buffer.from(await response.arrayBuffer()));
    } catch (e) {
      entry.status = 500;
      entry.error = { name: e.name, message: e.message };
      res.writeHead(500);
      res.end('fixture handler failure');
    } finally {
      if (entry.body?.runInput) initialFlowSettled = true;
    }
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  process.env.WORKFLOW_LOCAL_BASE_URL = `http://127.0.0.1:${server.address().port}`;
  world = createWorld({
    pool,
    jobPrefix: 'api_v5_',
    namespace: 'slice4c3',
    enableInvoke: false,
    recoverActiveRuns: false,
    applicationManagedShutdown: true,
    queueConcurrency: 5,
  });
  setWorld(world);
  proxy = await createCommitReplyProxy(
    process.env.SLICE4C3_URL,
    process.env.SLICE4C3_OUT + '.frames.jsonl'
  );
  proxyPool = new Pool({
    connectionString: proxy.url,
    ssl: false,
    max: 1,
    connectionTimeoutMillis: 10000,
  });
  out.poolEvents = [];
  proxyPool.on('connect', (client) =>
    client.on('error', (error) =>
      out.poolEvents.push({
        name: error.name,
        message: error.message,
        code: error.code,
        stack: error.stack,
      })
    )
  );
  handleConnectionErrors(proxyPool, () => false);
  const warm = await proxyPool.query(
    "SELECT pg_backend_pid() pid,current_setting('TimeZone') timezone,current_setting('search_path') search_path"
  );
  proxyPid = warm.rows[0].pid;
  out.proxyWarm = warm.rows[0];
  proxyStorage = createEventsStorage(createClient(proxyPool), {
    jobPrefix: 'api_v5_',
  });
  const realCreate = world.events.create.bind(world.events);
  world.events.create = async function (...args) {
    const [id, data] = args;
    if (
      id === runId &&
      ['step_completed', 'step_failed'].includes(data.eventType) &&
      out.injections === 0
    ) {
      const row = await observer.query(
        'SELECT step_name FROM workflow.workflow_steps WHERE step_id=$1',
        [data.correlationId]
      );
      if (row.rows[0]?.step_name === 'step//slice4c3//target') {
        out.injections++;
        out.stepId = data.correlationId;
        proxy.arm(proxyPid);
        let failure;
        try {
          await proxyStorage.create(...args);
        } catch (error) {
          failure = error;
        }
        out.proxy = proxy.snapshot();
        assert.ok(failure, 'selected storage call did not fail');
        const errorInfo = (error, depth = 0) =>
          error && depth < 5
            ? {
                name: error.name,
                message: error.message,
                code: error.code,
                stack: error.stack,
                cause: errorInfo(error.cause, depth + 1),
              }
            : null;
        out.clientError = errorInfo(failure);
        out.boundary = (
          await observer.query(
            `SELECT r.status::text run_status,s.step_id,s.status::text step_status,encode(s.output_cbor,'hex') output_cbor,encode(s.error_cbor,'hex') error_cbor,(SELECT jsonb_agg(jsonb_build_object('id',e.id,'type',e.type,'payload_cbor',encode(e.payload_cbor,'hex'),'payload',e.payload) ORDER BY e.id) FROM workflow.workflow_runs r0 JOIN workflow.workflow_events e ON e.run_id=r0.id WHERE r0.id=r.id AND e.correlation_id=s.step_id AND e.type IN ('step_completed','step_failed','step_retrying')) terminal FROM workflow.workflow_runs r JOIN workflow.workflow_steps s ON s.run_id=r.id WHERE r.id=$1 AND s.step_id=$2`,
            [id, data.correlationId]
          )
        ).rows[0];
        out.boundary.jobs = (
          await observer.query(
            'SELECT j.id::text,t.identifier,j.attempts,j.locked_by FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id ORDER BY j.id'
          )
        ).rows;
        assert.equal(out.proxy.cut, true);
        assert.deepEqual(
          out.proxy.events
            .filter((e) => e.suppressed)
            .map((e) => [e.type, e.tag ?? null]),
          [
            ['C', 'COMMIT'],
            ['Z', null],
          ]
        );
        assert.equal(out.boundary.run_status, 'running');
        assert.equal(out.boundary.terminal.length, 1);
        throw failure;
      }
    }
    try {
      return await realCreate(...args);
    } catch (e) {
      if (
        data.eventType === 'step_started' &&
        data.correlationId === out.stepId
      ) {
        out.startConflict = {
          name: e.name,
          message: e.message,
          isEntityConflict: EntityConflictError.is(e),
        };
      }
      if (
        data.eventType === 'step_failed' &&
        data.correlationId === out.stepId &&
        out.injections
      )
        out.retryTerminalConflict = {
          name: e.name,
          message: e.message,
          isEntityConflict: EntityConflictError.is(e),
        };
      throw e;
    }
  };
  const target = async () => {
    await bump('target');
    if (shape === 'queued') {
      for (let i = 0; i < 400; i++) {
        const q = await observer.query(
          "SELECT count(*)::int n FROM workflow.workflow_steps WHERE run_id=$1 AND step_name='step//slice4c3//sibling' AND status='completed'",
          [runId]
        );
        const queued = await observer.query(
          'SELECT count(*)::int n FROM graphile_worker._private_jobs'
        );
        if (q.rows[0].n === 3 && initialFlowSettled && queued.rows[0].n === 1) {
          out.release = { siblings: 3, initialFlowSettled, remainingJobs: 1 };
          break;
        }
        await pause(50);
      }
      assert.ok(
        out.release,
        'siblings/initial flow not settled before target release'
      );
    }
    if (outcome === 'fatal') throw new FatalError('slice4c3 fatal');
    if (outcome === 'exhausted') throw new Error('slice4c3 ordinary');
    return { fixed: 'slice4c3-success' };
  };
  if (outcome === 'exhausted') target.maxRetries = 0;
  registerStepFunction('step//slice4c3//target', target);
  registerStepFunction('step//slice4c3//sibling', async (label) => {
    await bump(label);
    return label;
  });
  registerStepFunction('step//slice4c3//continuation', async (observed) => {
    await bump('continuation');
    return { accepted: observed.kind };
  });
  const started = await start({ workflowId: 'workflow//slice4c3//main' }, [], {
    world,
    namespace: 'slice4c3',
  });
  runId = started.runId;
  out.runId = runId;
  out.worldSpec = world.specVersion;
  await world.start();
  out.stage = 'running';
  for (let i = 0; i < 1000; i++) {
    const r = await observer.query(
      'SELECT id,status::text,spec_version FROM workflow.workflow_runs WHERE id=$1',
      [runId]
    );
    const jobs = await observer.query(
      'SELECT j.id::text,t.identifier,j.attempts,j.locked_by,j.last_error FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id ORDER BY j.id'
    );
    if (r.rows[0]?.status === 'completed' && jobs.rows.length === 0) {
      out.run = r.rows[0];
      out.jobs = jobs.rows;
      break;
    }
    if (r.rows[0]?.status === 'failed') {
      out.failedRun = r.rows[0];
      break;
    }
    await pause(50);
  }
  out.effects = (
    await observer.query(
      'SELECT label,count FROM public.slice4c3_effects ORDER BY label'
    )
  ).rows;
  if (out.stepId)
    out.after = (
      await observer.query(
        `SELECT r.status::text run_status,s.step_id,s.status::text step_status,encode(s.output_cbor,'hex') output_cbor,encode(s.error_cbor,'hex') error_cbor,(SELECT jsonb_agg(jsonb_build_object('id',e.id,'type',e.type,'payload_cbor',encode(e.payload_cbor,'hex'),'payload',e.payload) ORDER BY e.id) FROM workflow.workflow_events e WHERE e.run_id=r.id AND e.correlation_id=s.step_id AND e.type IN ('step_completed','step_failed','step_retrying')) terminal FROM workflow.workflow_runs r JOIN workflow.workflow_steps s ON s.run_id=r.id WHERE r.id=$1 AND s.step_id=$2`,
        [runId, out.stepId]
      )
    ).rows[0];
  assert.ok(out.run, 'run did not complete and drain naturally');
  out.proxyReconnect = (
    await proxyPool.query('SELECT pg_backend_pid() pid')
  ).rows[0];
  assert.notEqual(out.proxyReconnect.pid, proxyPid);
  out.result = await api.getRun(runId).returnValue;
  out.worldSteps = (
    await world.steps.list({
      runId,
      pagination: { limit: 100 },
      resolveData: 'all',
    })
  ).data.map((s) => ({ id: s.stepId, name: s.stepName, status: s.status }));
  out.worldEvents = (
    await world.events.list({
      runId,
      pagination: { limit: 100 },
      resolveData: 'none',
    })
  ).data.map((e) => ({
    id: e.eventId,
    type: e.eventType,
    correlationId: e.correlationId,
  }));
  assert.equal(out.injections, 1);
  assert.equal(out.after.terminal.length, 1);
  assert.deepEqual(out.after.terminal, out.boundary.terminal);
  assert.equal(out.after.output_cbor, out.boundary.output_cbor);
  assert.equal(out.after.error_cbor, out.boundary.error_cbor);
  assert.equal(out.effects.find((x) => x.label === 'target')?.count, 1);
  assert.equal(out.effects.find((x) => x.label === 'continuation')?.count, 1);
  out.exit = 0;
  out.stage = 'complete';
} catch (e) {
  out.error = { name: e.name, message: e.message, stack: e.stack };
  console.error(e);
} finally {
  for (const close of [
    async () => world?.close(),
    async () => proxyPool?.end(),
    async () => proxy?.close(),
    async () => {
      if (server) await new Promise((resolve) => server.close(resolve));
    },
    async () => pool.end(),
    async () => observer.end(),
  ]) {
    try {
      await close();
    } catch (e) {
      (out.cleanupErrors ??= []).push({ name: e.name, message: e.message });
      out.exit = 1;
    }
  }
  hook.deregister();
  out.loaded = [];
  for (const file of [...loaded].sort()) {
    const data = await readFile(file);
    const entry = {
      file,
      sha256: createHash('sha256').update(data).digest('hex'),
    };
    if (file.endsWith('.js') || file.endsWith('.mjs')) {
      const m = data
        .toString()
        .match(
          /^\/\/# sourceMappingURL=data:application\/json;base64,([A-Za-z0-9+/=]+)$/m
        );
      if (m) {
        const map = JSON.parse(Buffer.from(m[1], 'base64').toString());
        entry.sourceMap = (map.sources || []).map((name, i) => ({
          name,
          sha256:
            map.sourcesContent?.[i] === undefined
              ? null
              : createHash('sha256')
                  .update(map.sourcesContent[i])
                  .digest('hex'),
        }));
      }
    }
    out.loaded.push(entry);
  }
  await writeFile(process.env.SLICE4C3_OUT, JSON.stringify(out, null, 2));
}
if (out.exit !== 0) process.exitCode = 1;
