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
const req = createRequire(process.env.SLICE4C4_TARGET + '/package.json');
const runtimePath = req.resolve('workflow/runtime');
const corePath = createRequire(runtimePath).resolve('@workflow/core/runtime');
const coreDir = dirname(corePath);
const worldPath =
  process.env.SLICE4C4_SOURCE + '/packages/world-postgres/dist/index.js';
const { Pool } = createRequire(
  process.env.SLICE4C4_SOURCE + '/packages/world-postgres/package.json'
)('pg');
const { createWorld } = await import(pathToFileURL(worldPath));
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
const pool = new Pool({ connectionString: process.env.SLICE4C4_URL });
const observer = new Pool({ connectionString: process.env.SLICE4C4_URL });
const shape = process.env.SLICE4C4_SHAPE,
  mode = process.env.SLICE4C4_MODE;
const out = {
  exit: 1,
  stage: 'init',
  shape,
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
  runId,
  initialFlowSettled = false,
  streamController,
  streamStarted,
  streamClosed = false;
const closeStream = () => {
  if (!streamClosed && streamController) {
    streamClosed = true;
    try {
      streamController.close();
      out.streamClosed = Date.now();
    } catch (e) {
      out.streamCloseError = e.message;
    }
  }
};
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
const bump = async (label) =>
  (
    await pool.query(
      'INSERT INTO public.slice4c4_effects VALUES ($1,1) ON CONFLICT(label) DO UPDATE SET count=public.slice4c4_effects.count+1 RETURNING count',
      [label]
    )
  ).rows[0].count;
try {
  assert.equal(out.settings.inlineLimit, 3);
  out.server = (
    await observer.query(
      "SELECT version() version, current_setting('TimeZone') timezone, current_setting('search_path') search_path, pg_backend_pid() pid"
    )
  ).rows[0];
  const code = `globalThis.__private_workflows=new Map();const target=globalThis[Symbol.for('WORKFLOW_USE_STEP')]('step//slice4c4//target');const sibling=globalThis[Symbol.for('WORKFLOW_USE_STEP')]('step//slice4c4//sibling');const continuation=globalThis[Symbol.for('WORKFLOW_USE_STEP')]('step//slice4c4//continuation');globalThis.__private_workflows.set('workflow//slice4c4//main',async()=>{const stream=${shape === 'queued' ? "(await Promise.all([sibling('s1'),sibling('s2'),sibling('s3'),target()]))[3]" : 'await target()'};return await continuation(stream)});`;

  const flow = runtime.workflowEntrypoint(code, { namespace: 'slice4c4' });
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
          `SELECT j.id::text,t.identifier,j.attempts,j.locked_by,j.last_error FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id WHERE j.payload->>'messageId'=$1`,
          [req.headers['x-vqs-message-id']]
        )
      ).rows[0];
      if (
        mode === 'delivery' &&
        out.wakeMessageId === req.headers['x-vqs-message-id'] &&
        !out.injections
      ) {
        out.injections++;
        out.faultDelivery = entry;
        out.deliveryBoundary = (
          await observer.query(
            `SELECT r.status::text run_status,s.status::text step_status,encode(s.output_cbor,'hex') output_cbor,(SELECT count(*)::int FROM workflow.workflow_events e WHERE e.run_id=r.id AND e.correlation_id=s.step_id AND e.type='step_completed') terminal_count FROM workflow.workflow_runs r JOIN workflow.workflow_steps s ON s.run_id=r.id WHERE r.id=$1 AND s.step_id=$2`,
            [runId, out.stepId]
          )
        ).rows[0];
        entry.status = 503;
        closeStream();
        res.writeHead(503);
        res.end('injected wake delivery failure');
        return;
      }
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
    namespace: 'slice4c4',
    enableInvoke: false,
    recoverActiveRuns: false,
    applicationManagedShutdown: true,
    queueConcurrency: 5,
  });
  setWorld(world);
  const realQueue = world.queue.bind(world);
  world.queue = async function (...args) {
    const [name, data] = args;
    if (
      data.runId === runId &&
      !data.stepId &&
      !data.runInput &&
      !out.wakeCall
    ) {
      const rows = await observer.query(
        `SELECT r.status::text run_status,s.step_id,s.status::text step_status,encode(s.output_cbor,'hex') output_cbor,encode(s.error_cbor,'hex') error_cbor,(SELECT jsonb_agg(jsonb_build_object('id',e.id,'type',e.type,'payload_cbor',encode(e.payload_cbor,'hex'),'payload',e.payload) ORDER BY e.id) FROM workflow.workflow_events e WHERE e.run_id=r.id AND e.correlation_id=s.step_id AND e.type IN ('step_completed','step_failed','step_retrying')) terminal FROM workflow.workflow_runs r JOIN workflow.workflow_steps s ON s.run_id=r.id WHERE r.id=$1 AND s.step_name='step//slice4c4//target' AND s.status='completed'`,
        [runId]
      );
      if (rows.rows.length) {
        out.stepId = rows.rows[0].step_id;
        out.boundary = rows.rows[0];
        out.boundary.jobs = (
          await observer.query(
            'SELECT j.id::text,t.identifier,j.attempts,j.locked_by,j.payload FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id ORDER BY j.id'
          )
        ).rows;
        out.wakeCall = {
          name,
          data,
          options: args[2],
          elapsedMs: Date.now() - streamStarted,
        };
        assert.equal(out.boundary.run_status, 'running');
        assert.equal(out.boundary.terminal.length, 1);
        assert.ok(out.wakeCall.elapsedMs >= 500);
        if (mode === 'publication') {
          out.injections++;
          closeStream();
          throw new Error('slice4c4 injected wake publication failure');
        }
        const result = await realQueue(...args);
        out.wakeMessageId = result.messageId;
        out.queueResult = result;
        return result;
      }
    }
    return realQueue(...args);
  };
  const realCreate = world.events.create.bind(world.events);
  world.events.create = async function (...args) {
    try {
      return await realCreate(...args);
    } catch (e) {
      if (
        args[1].eventType === 'step_started' &&
        args[1].correlationId === out.stepId
      )
        out.startConflict = {
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
          "SELECT count(*)::int n FROM workflow.workflow_steps WHERE run_id=$1 AND step_name='step//slice4c4//sibling' AND status='completed'",
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
    streamStarted = Date.now();
    return new ReadableStream({
      start(controller) {
        streamController = controller;
        controller.enqueue(new TextEncoder().encode('slice4c4-stream'));
      },
      cancel(reason) {
        out.streamCancel = String(reason);
      },
    });
  };
  registerStepFunction('step//slice4c4//target', target);
  registerStepFunction('step//slice4c4//sibling', async (label) => {
    await bump(label);
    return label;
  });
  registerStepFunction('step//slice4c4//continuation', async (stream) => {
    await bump('continuation');
    const reader = stream.getReader();
    const chunks = [];
    for (;;) {
      const item = await reader.read();
      if (item.done) break;
      chunks.push(item.value);
    }
    return new TextDecoder().decode(Buffer.concat(chunks));
  });
  const started = await start({ workflowId: 'workflow//slice4c4//main' }, [], {
    world,
    namespace: 'slice4c4',
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
      'SELECT label,count FROM public.slice4c4_effects ORDER BY label'
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
  assert.equal(out.result, 'slice4c4-stream');
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
  try {
    closeStream();
    await world?.close();
    if (server) await new Promise((r) => server.close(r));
    await pool.end();
    await observer.end();
  } catch (e) {
    out.cleanupError = { name: e.name, message: e.message };
    out.exit = 1;
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
  await writeFile(process.env.SLICE4C4_OUT, JSON.stringify(out, null, 2));
}
if (out.exit !== 0) process.exitCode = 1;
