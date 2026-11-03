import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { Pool } from 'pg';
import { expect, it } from 'vitest';

const adminUrl = process.env.SLICE4C_ADMIN_URL;
const receipts = process.env.SLICE4C_RECEIPTS;
const pulsify = process.env.SLICE4C_PULSIFY;
const target = process.env.SLICE4C_TARGET;
const source = resolve('../..');
const cleanEnv = Object.fromEntries(
  Object.entries(process.env).filter(([key]) => !key.startsWith('WORKFLOW_'))
);

it.skipIf(!adminUrl || !receipts || !pulsify || !target)(
  'isolates real v4 and v5 jobs by Graphile task prefix, with same-prefix claim controls',
  async () => {
    if (!adminUrl || !receipts || !pulsify || !target)
      throw new Error('Missing native protocol fixture configuration');
    if (!['127.0.0.1', 'localhost', '::1'].includes(new URL(adminUrl).hostname))
      throw new Error('Postgres must be loopback');
    await mkdir(receipts, { recursive: true });
    const admin = new Pool({ connectionString: adminUrl });
    const pools: Pool[] = [];
    const result: Record<string, unknown> = {};
    const run = async (name: string, bin: string, env: NodeJS.ProcessEnv) => {
      const child = spawn(process.execPath, [bin], {
        env,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let stdout = '',
        stderr = '';
      child.stdout.on('data', (data: Buffer) => {
        stdout += data.toString();
      });
      child.stderr.on('data', (data: Buffer) => {
        stderr += data.toString();
      });
      const outcome = await new Promise<{
        code: number | null;
        signal: NodeJS.Signals | null;
      }>((resolve, reject) => {
        child.once('error', reject);
        child.once('close', (code, signal) => resolve({ code, signal }));
      });
      await writeFile(join(receipts, `${name}.log`), stdout + stderr);
      await writeFile(
        join(receipts, `${name}.exit.json`),
        JSON.stringify(outcome)
      );
      expect(outcome).toEqual({ code: 0, signal: null });
    };
    const startHeldTrap = async (
      name: string,
      bin: string,
      env: NodeJS.ProcessEnv
    ) => {
      const child = spawn(process.execPath, [bin], {
        env,
        stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      });
      let stdout = '',
        stderr = '';
      child.stdout.on('data', (data: Buffer) => {
        stdout += data.toString();
      });
      child.stderr.on('data', (data: Buffer) => {
        stderr += data.toString();
      });
      const close = new Promise<{
        code: number | null;
        signal: NodeJS.Signals | null;
      }>((resolve, reject) => {
        child.once('error', reject);
        child.once('close', (code, signal) => resolve({ code, signal }));
      });
      const ready = await new Promise<unknown>((resolve, reject) => {
        child.once('message', resolve);
        close.then(
          (outcome) =>
            reject(
              new Error(
                `Foreign queue exited before ready: ${JSON.stringify(outcome)} ${stderr}`
              )
            ),
          reject
        );
      });
      return {
        child,
        ready,
        stop: async () => {
          if (child.connected) child.send({ stop: true });
          const outcome = await close;
          await writeFile(join(receipts, `${name}.log`), stdout + stderr);
          await writeFile(
            join(receipts, `${name}.exit.json`),
            JSON.stringify(outcome)
          );
          expect(outcome).toEqual({ code: 0, signal: null });
        },
      };
    };
    try {
      for (const [lane, negative] of [
        ['v4', false],
        ['v5', false],
        ['v4', true],
        ['v5', true],
      ] as const) {
        const label = `${lane}_${negative ? 'negative' : 'positive'}`;
        const db = `slice4c_${label}_${randomBytes(4).toString('hex')}`;
        await admin.query(`CREATE DATABASE "${db}"`);
        const url = new URL(adminUrl);
        url.pathname = `/${db}`;
        const pool = new Pool({ connectionString: url.toString() });
        pools.push(pool);
        const baseEnv: NodeJS.ProcessEnv = {
          ...cleanEnv,
          TZ: 'UTC',
          WORKFLOW_QUEUE_NAMESPACE: 'slice4c',
          WORKFLOW_SPEC_VERSION: '8',
          WORKFLOW_TRACE_MODE: 'continuous',
          WORKFLOW_INLINE_EARLY_ADVANCE: '0',
          WORKFLOW_POSTGRES_INVOKE: '0',
          WORKFLOW_POSTGRES_EXPERIMENTAL_START_HOOK: '0',
          WORKFLOW_SKIP_BOOT_RECOVERY: '1',
          WORKFLOW_POSTGRES_JOB_PREFIX: 'api_v5_',
          WORKFLOW_POSTGRES_URL: url.toString(),
          SLICE4C_URL: url.toString(),
          SLICE4C_SOURCE: source,
          SLICE4C_PULSIFY: pulsify,
          SLICE4C_TARGET: target,
        };
        const bootstrap =
          lane === 'v4'
            ? join(
                pulsify,
                'apps/api/node_modules/@workflow/world-postgres/bin/setup.js'
              )
            : resolve('bin/setup.js');
        await run(`${label}-bootstrap`, bootstrap, baseEnv);
        await pool.query(
          'CREATE TABLE public.slice4c_effects (label text PRIMARY KEY, count integer NOT NULL)'
        );
        const producerPath = resolve(
          `src/__fixtures__/protocol-native-${lane}.mjs`
        );
        const foreignLane = lane === 'v4' ? 'v5' : 'v4';
        const foreignPath = resolve(
          `src/__fixtures__/protocol-native-${foreignLane}.mjs`
        );
        const prefix = lane === 'v4' ? 'api_' : 'api_v5_';
        const producerFile = join(receipts, `${label}-producer.json`);
        await run(`${label}-producer`, producerPath, {
          ...baseEnv,
          SLICE4C_ROLE: 'producer',
          SLICE4C_PREFIX: prefix,
          SLICE4C_OUT: producerFile,
        });
        const producer = JSON.parse(await readFile(producerFile, 'utf8'));
        expect(producer.exit).toBe(0);
        expect(producer.specVersion).toBe(lane === 'v4' ? 3 : 8);
        expect(producer.namespace).toBe('slice4c');
        expect(producer.settings).toEqual({
          spec: '8',
          trace: 'continuous',
          earlyAdvance: '0',
          invoke: '0',
          startHook: '0',
          skipRecovery: '1',
          envPrefix: 'api_v5_',
          configPrefix: prefix,
          configNamespace: 'slice4c',
          enableInvoke: false,
          recoverActiveRuns: false,
          applicationManagedShutdown: true,
          maxInlineOverride: null,
        });
        expect(producer.worldSpecVersion).toBe(lane === 'v4' ? 3 : 8);
        expect(producer.capabilities).toEqual({
          invoke: false,
          startHook: false,
        });
        expect(
          producer.loaded.some(
            (entry: { file: string }) => entry.file === producer.paths.worldPath
          )
        ).toBe(true);
        expect(
          producer.loaded.some(
            (entry: { file: string }) => entry.file === producer.paths.queuePath
          )
        ).toBe(true);
        const jobs = (
          await pool.query(
            'SELECT j.id::text,t.identifier,j.attempts,j.locked_by,j.payload,to_jsonb(j) job_row FROM graphile_worker._private_jobs j JOIN graphile_worker._private_tasks t ON t.id=j.task_id ORDER BY j.id'
          )
        ).rows;
        expect(jobs).toHaveLength(1);
        expect(jobs[0]).toMatchObject({
          identifier: `${prefix}flows`,
          attempts: 0,
          locked_by: null,
        });
        const trapFile = join(receipts, `${label}-trap.json`);
        const foreignPrefix = negative
          ? prefix
          : lane === 'v4'
            ? 'api_v5_'
            : 'api_';
        const trapEnv = {
          ...baseEnv,
          SLICE4C_ROLE: 'trap',
          SLICE4C_PREFIX: foreignPrefix,
          SLICE4C_NEGATIVE: negative ? '1' : '0',
          SLICE4C_JOB: jobs[0].id,
          SLICE4C_OUT: trapFile,
        };
        if (negative) {
          await run(`${label}-trap`, foreignPath, trapEnv);
          const trap = JSON.parse(await readFile(trapFile, 'utf8'));
          expect(trap.exit).toBe(0);
          expect(trap.effects).toEqual([]);
          expect(trap.claim.id).toBe(jobs[0].id);
          expect(
            trap.claim.attempts > 0 ||
              trap.claim.locked_by ||
              trap.claim.last_error
          ).toBeTruthy();
          expect(
            trap.deliveries.some(
              (d: { body: { runId?: string } }) =>
                d.body?.runId === producer.runId
            ) || typeof trap.claim.last_error === 'string'
          ).toBe(true);
          result[label] = { db, producer, jobs, trap };
          continue;
        }
        const held = await startHeldTrap(`${label}-trap`, foreignPath, {
          ...trapEnv,
          SLICE4C_HOLD: '1',
        });
        let trap;
        let consumer;
        let unchanged;
        try {
          expect(held.child.pid).toBeDefined();
          unchanged = (
            await pool.query(
              'SELECT id::text,attempts,locked_by,to_jsonb(j) job_row FROM graphile_worker._private_jobs j WHERE id=$1',
              [jobs[0].id]
            )
          ).rows;
          expect(unchanged).toEqual([
            {
              id: jobs[0].id,
              attempts: 0,
              locked_by: null,
              job_row: jobs[0].job_row,
            },
          ]);
          const consumerFile = join(receipts, `${label}-consumer.json`);
          await run(`${label}-consumer`, producerPath, {
            ...baseEnv,
            SLICE4C_ROLE: 'consumer',
            SLICE4C_PREFIX: prefix,
            SLICE4C_RUN: producer.runId,
            SLICE4C_OUT: consumerFile,
          });
          expect(held.child.exitCode).toBeNull();
          consumer = JSON.parse(await readFile(consumerFile, 'utf8'));
        } finally {
          await held.stop();
        }
        trap = JSON.parse(await readFile(trapFile, 'utf8'));
        expect(trap.exit).toBe(0);
        expect(trap.isolationEffects).toEqual([]);
        expect(trap.deliveries).toHaveLength(1);
        expect(trap.deliveries[0].body.runId).toBe(`wrun_probe_${foreignLane}`);
        expect(trap.deliveries[0].headers['x-vqs-message-attempt']).toBe('1');
        expect(trap.deliveries[0].locks).toEqual([
          expect.objectContaining({
            identifier: `${foreignPrefix}flows`,
            attempts: 1,
            locked_by: expect.any(String),
          }),
        ]);
        expect(
          trap.loaded.some(
            (entry: { file: string }) => entry.file === trap.paths.queuePath
          )
        ).toBe(true);
        expect(
          trap.loaded.some(
            (entry: { file: string }) => entry.file === trap.paths.worldPath
          )
        ).toBe(false);
        expect(consumer.exit).toBe(0);
        expect(consumer.run).toMatchObject({
          id: producer.runId,
          status: 'completed',
          spec_version: lane === 'v4' ? '3' : '8',
        });
        expect(consumer.finalJobs).toEqual([]);
        expect(consumer.effects).toEqual(
          (lane === 'v4' ? ['one'] : ['a', 'b', 'c', 'd']).map((label) => ({
            label,
            count: 1,
          }))
        );
        expect(consumer.result).toEqual(
          lane === 'v4'
            ? { label: 'one', count: 1 }
            : ['a', 'b', 'c', 'd'].map((label) => ({ label, count: 1 }))
        );
        if (lane === 'v4') {
          expect(
            consumer.deliveries.some((d: { path: string }) =>
              d.path.includes('/flow')
            )
          ).toBe(true);
          expect(
            consumer.deliveries.some((d: { path: string }) =>
              d.path.includes('/step')
            )
          ).toBe(true);
          expect(consumer.stepJob).toMatchObject({
            identifier: 'api_steps',
            attempts: 1,
            locked_by: expect.any(String),
          });
        } else {
          expect(consumer.inlineLimit).toBe(3);
          expect(
            consumer.deliveries.every((d: { path: string }) =>
              d.path.includes('/flow')
            )
          ).toBe(true);
          const queuedStep = consumer.deliveries.find(
            (d: { body: { stepId?: string } }) =>
              typeof d.body?.stepId === 'string'
          );
          expect(queuedStep.body.runId).toBe(producer.runId);
          expect(queuedStep.headers['x-vqs-message-attempt']).toBe('1');
          expect(queuedStep.locks).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                identifier: 'api_v5_flows',
                attempts: 1,
                locked_by: expect.any(String),
              }),
            ])
          );
          expect(
            consumer.steps.map((step: { id: string }) => step.id)
          ).toContain(queuedStep.body.stepId);
        }
        result[label] = {
          db,
          producer,
          jobs,
          trap,
          unchanged,
          consumer,
          overlap: {
            foreignPid: held.child.pid,
            foreignLiveThroughConsumer: true,
          },
        };
      }
      await writeFile(
        join(receipts, 'assertions.json'),
        JSON.stringify({ exit: 0, ...result }, null, 2)
      );
    } finally {
      await Promise.all(pools.map((pool) => pool.end()));
      await admin.end();
    }
  },
  240_000
);
