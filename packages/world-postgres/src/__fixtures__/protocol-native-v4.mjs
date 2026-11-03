import assert from 'node:assert/strict';
import { createRequire, registerHooks } from 'node:module';
import { createServer } from 'node:http';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { dirname, join } from 'node:path';
import { readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
const loaded = new Set();
const hook = registerHooks({
  load(url, context, nextLoad) {
    if (url.startsWith('file:')) loaded.add(fileURLToPath(url));
    return nextLoad(url, context);
  },
});
const lane = 'v4';
const old = lane === 'v4';
const root = old
  ? process.env.SLICE4C_PULSIFY + '/apps/api'
  : process.env.SLICE4C_TARGET;
const req = createRequire(root + '/package.json');
const runtimePath = req.resolve('workflow/runtime');
const corePath = createRequire(runtimePath).resolve('@workflow/core/runtime');
const coreDir = dirname(corePath);
const worldPath = old
  ? req.resolve('@workflow/world-postgres')
  : process.env.SLICE4C_SOURCE + '/packages/world-postgres/dist/index.js';
const queuePath = join(dirname(worldPath), 'queue.js');
const { Pool } = createRequire(
  process.env.SLICE4C_SOURCE + '/packages/world-postgres/package.json'
)('pg');
const role = process.env.SLICE4C_ROLE;
const { createQueue } = await import(pathToFileURL(queuePath));
const worldPkg = await import(
  pathToFileURL(createRequire(worldPath).resolve('@workflow/world'))
);
let createWorld, setWorld, runtime, registerStepFunction, start, api;
if (role !== 'trap') {
  ({ createWorld } = await import(pathToFileURL(worldPath)));
  ({ setWorld } = await import(
    pathToFileURL(old ? join(coreDir, 'runtime/world.js') : corePath)
  ));
  runtime = await import(pathToFileURL(runtimePath));
  ({ registerStepFunction } = await import(
    pathToFileURL(join(coreDir, 'private.js'))
  ));
  ({ start } = await import(pathToFileURL(join(coreDir, 'runtime/start.js'))));
  api = await import(pathToFileURL(req.resolve('workflow/api')));
}
const pool = new Pool({ connectionString: process.env.SLICE4C_URL });
const prefix = process.env.SLICE4C_PREFIX;
const namespace = 'slice4c';
const config = {
  pool,
  jobPrefix: prefix,
  namespace,
  enableInvoke: false,
  recoverActiveRuns: false,
  applicationManagedShutdown: true,
  queueConcurrency: 4,
};
const out = {
  exit: 1,
  lane,
  role,
  pid: process.pid,
  prefix,
  namespace,
  settings: {
    spec: process.env.WORKFLOW_SPEC_VERSION,
    trace: process.env.WORKFLOW_TRACE_MODE,
    earlyAdvance: process.env.WORKFLOW_INLINE_EARLY_ADVANCE,
    invoke: process.env.WORKFLOW_POSTGRES_INVOKE,
    startHook: process.env.WORKFLOW_POSTGRES_EXPERIMENTAL_START_HOOK,
    skipRecovery: process.env.WORKFLOW_SKIP_BOOT_RECOVERY,
    envPrefix: process.env.WORKFLOW_POSTGRES_JOB_PREFIX,
    configPrefix: config.jobPrefix,
    configNamespace: config.namespace,
    enableInvoke: config.enableInvoke,
    recoverActiveRuns: config.recoverActiveRuns,
    applicationManagedShutdown: config.applicationManagedShutdown,
    maxInlineOverride: process.env.WORKFLOW_MAX_INLINE_STEPS ?? null,
  },
  paths: { runtimePath, corePath, worldPath, queuePath },
  deliveries: [],
};
let server, world, queue;
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
try {
  if (role === 'producer') {
    world = createWorld(config);
    setWorld(world);
    const run = await start({ workflowId: 'workflow//slice4c//main' }, [], {
      world,
      namespace,
    });
    const record = await world.runs.get(run.runId);
    out.runId = run.runId;
    out.specVersion = record.specVersion;
    out.worldSpecVersion = world.specVersion;
    out.capabilities = {
      invoke: typeof world.invoke === 'function',
      startHook: typeof world.startHook === 'function',
    };
  } else if (role === 'trap') {
    server = createServer(async (req, res) => {
      const chunks = [];
      for await (const c of req) chunks.push(c);
      const raw = Buffer.concat(chunks).toString();
      let body;
      try {
        body = JSON.parse(raw);
      } catch {
        body = raw;
      }
      const locks = (
        await pool.query(
          'SELECT j.id::text,t.identifier,j.attempts,j.locked_by FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id WHERE j.locked_by IS NOT NULL ORDER BY j.id'
        )
      ).rows;
      out.deliveries.push({ path: req.url, headers: req.headers, body, locks });
      res.writeHead(process.env.SLICE4C_NEGATIVE === '1' ? 503 : 200);
      res.end('trap');
    });
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    process.env.WORKFLOW_LOCAL_BASE_URL = `http://127.0.0.1:${server.address().port}`;
    queue = createQueue(config, pool);
    await queue.start();
    if (process.env.SLICE4C_NEGATIVE !== '1') {
      const marker = `wrun_probe_${lane}`;
      const own = await queue.queue(
        `${worldPkg.getQueueTopicPrefix('workflow', namespace)}probe`,
        { runId: marker }
      );
      out.probe = { marker, messageId: own.messageId };
      for (let i = 0; i < 100 && !out.deliveries.length; i++) await pause(50);
      assert.equal(
        out.deliveries.length,
        1,
        'own probe did not reach live foreign queue'
      );
      await pause(1500);
      out.isolationEffects = (
        await pool.query(
          'SELECT label,count FROM public.slice4c_effects ORDER BY label'
        )
      ).rows;
      out.isolationJobs = (
        await pool.query(
          'SELECT j.id::text,t.identifier,j.attempts,j.locked_by FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id ORDER BY j.id'
        )
      ).rows;
      if (process.env.SLICE4C_HOLD === '1') {
        assert.ok(process.send, 'held trap requires IPC');
        process.send({
          ready: true,
          pid: process.pid,
          probe: out.probe,
          deliveries: out.deliveries,
          effects: out.isolationEffects,
          jobs: out.isolationJobs,
        });
        await new Promise((resolve, reject) => {
          const timeout = setTimeout(
            () => reject(new Error('trap hold timeout')),
            45000
          );
          process.once('message', () => {
            clearTimeout(timeout);
            resolve();
          });
          process.once('disconnect', () => {
            clearTimeout(timeout);
            reject(new Error('trap IPC disconnected'));
          });
        });
      }
    } else {
      for (let i = 0; i < 160; i++) {
        const q = await pool.query(
          'SELECT id::text,attempts,locked_by,last_error FROM graphile_worker._private_jobs WHERE id=$1',
          [process.env.SLICE4C_JOB]
        );
        if (
          q.rows[0] &&
          (q.rows[0].attempts > 0 ||
            q.rows[0].locked_by ||
            q.rows[0].last_error)
        ) {
          out.claim = q.rows[0];
          break;
        }
        await pause(50);
      }
      assert.ok(out.claim, 'same-prefix adapter did not claim producer job');
    }
    const jobs = await pool.query(
      'SELECT j.id::text,t.identifier,j.attempts,j.locked_by,j.last_error FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id ORDER BY j.id'
    );
    out.jobs = jobs.rows;
    out.effects = (
      await pool.query(
        'SELECT label,count FROM public.slice4c_effects ORDER BY label'
      )
    ).rows;
  } else if (role === 'consumer') {
    const code = `globalThis.__private_workflows=new Map();const step=globalThis[Symbol.for('WORKFLOW_USE_STEP')]('step//slice4c//business');globalThis.__private_workflows.set('workflow//slice4c//main',async()=>${old ? "await step('one')" : "await Promise.all(['a','b','c','d'].map(x=>step(x)))"});`;
    const flow = runtime.workflowEntrypoint(code, { namespace });
    const stepHandler = runtime.stepEntrypoint;
    server = createServer(async (req, res) => {
      try {
        const chunks = [];
        for await (const c of req) chunks.push(c);
        const bytes = Buffer.concat(chunks);
        let body;
        try {
          body = JSON.parse(bytes.toString());
        } catch {
          body = null;
        }
        const locks = (
          await pool.query(
            'SELECT j.id::text,t.identifier,j.attempts,j.locked_by FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id WHERE j.locked_by IS NOT NULL ORDER BY j.id'
          )
        ).rows;
        out.deliveries.push({
          path: req.url,
          headers: req.headers,
          body,
          locks,
        });
        if (old && req.url?.includes('/step')) await pause(300);
        const request = new Request('http://127.0.0.1' + req.url, {
          method: req.method,
          headers: req.headers,
          body: bytes,
        });
        const response = await (old && req.url?.includes('/step')
          ? stepHandler
          : flow)(request);
        res.writeHead(response.status, Object.fromEntries(response.headers));
        res.end(Buffer.from(await response.arrayBuffer()));
      } catch (error) {
        console.error(error);
        res.writeHead(500);
        res.end('failure');
      }
    });
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    process.env.WORKFLOW_LOCAL_BASE_URL = `http://127.0.0.1:${server.address().port}`;
    world = createWorld(config);
    setWorld(world);
    registerStepFunction('step//slice4c//business', async (label) => {
      const row = await pool.query(
        'INSERT INTO public.slice4c_effects VALUES ($1,1) ON CONFLICT(label) DO UPDATE SET count=public.slice4c_effects.count+1 RETURNING count',
        [label]
      );
      return { label, count: row.rows[0].count };
    });
    await world.start();
    for (let i = 0; i < 400; i++) {
      const run = await pool.query(
        'SELECT id,status::text,spec_version FROM workflow.workflow_runs WHERE id=$1',
        [process.env.SLICE4C_RUN]
      );
      const jobs = await pool.query(
        'SELECT j.id::text,t.identifier,j.attempts,j.locked_by FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id ORDER BY j.id'
      );
      if (jobs.rows.some((j) => j.identifier.endsWith('steps')))
        out.stepJob = jobs.rows.find((j) => j.identifier.endsWith('steps'));
      if (run.rows[0]?.status === 'completed' && jobs.rows.length === 0) {
        out.run = run.rows[0];
        out.finalJobs = jobs.rows;
        break;
      }
      await pause(50);
    }
    assert.ok(out.run, 'run did not complete and drain naturally');
    out.result = await api.getRun(process.env.SLICE4C_RUN).returnValue;
    out.effects = (
      await pool.query(
        'SELECT label,count FROM public.slice4c_effects ORDER BY label'
      )
    ).rows;
    out.steps = (
      await world.steps.list({
        runId: process.env.SLICE4C_RUN,
        pagination: { limit: 100 },
      })
    ).data.map((s) => ({ id: s.stepId, status: s.status }));
    if (!old)
      out.inlineLimit = (
        await import(pathToFileURL(join(coreDir, 'runtime/constants.js')))
      ).getMaxInlineSteps();
  } else throw new Error('unknown role');
  out.exit = 0;
} catch (error) {
  out.error = { message: error.message, stack: error.stack };
  console.error(error);
} finally {
  try {
    await queue?.close();
    await world?.close();
    if (server) await new Promise((r) => server.close(r));
    await pool.end();
  } catch (error) {
    out.exit = 1;
    out.cleanupError = { message: error.message, stack: error.stack };
  }
  hook.deregister();
  out.loaded = [];
  for (const file of [...loaded].sort()) {
    const content = await readFile(file);
    const entry = {
      file,
      sha256: createHash('sha256').update(content).digest('hex'),
    };
    if (file.endsWith('.js') || file.endsWith('.mjs')) {
      const match = content
        .toString()
        .match(
          /^\/\/# sourceMappingURL=data:application\/json;base64,([A-Za-z0-9+/=]+)$/m
        );
      if (match) {
        const map = JSON.parse(Buffer.from(match[1], 'base64').toString());
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
  await writeFile(process.env.SLICE4C_OUT, JSON.stringify(out, null, 2));
  if (process.connected) process.disconnect();
}
if (out.exit !== 0) process.exitCode = 1;
