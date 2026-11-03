import { execFile, execFileSync, spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { createHash, randomBytes } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';
import { Pool } from 'pg';
import { expect, it } from 'vitest';
import { z } from 'zod';
import { inspectWorkflowDatabase } from './cli.js';

const adminUrl = process.env.WORKFLOW_NATIVE_HISTORY_ADMIN_URL;
const pulsify = process.env.WORKFLOW_SLICE4A_PULSIFY;
const target = process.env.WORKFLOW_SLICE4A_TARGET;
const container = process.env.WORKFLOW_NATIVE_HISTORY_CONTAINER;
const receipt = process.env.WORKFLOW_SLICE4A_RECEIPT;
const exec = promisify(execFile);
const oldResult = z.object({
  exit: z.literal(0),
  ids: z.record(z.string(), z.string()),
  partial: z.object({
    hooks: z
      .array(z.object({ run_id: z.string(), token: z.string() }))
      .length(2),
    sleepWaits: z.literal(1),
    retryPending: z.literal(1),
    rootEffect: z.literal(1),
  }),
  drain: z.object({
    counts: z.object({ active: z.literal(4), legacyJobs: z.number().min(1) }),
    unchanged: z.literal(true),
    timeout: z.literal(true),
    before: z.unknown(),
    after: z.unknown(),
  }),
  finalCounts: z.object({
    active: z.literal(0),
    legacyJobs: z.literal(0),
    newJobs: z.literal(0),
  }),
  effects: z.array(z.object({ label: z.string(), count: z.number().int() })),
  restart: z.object({
    oldPid: z.number().int(),
    newPid: z.number().int(),
    beforeStart: z.object({
      sleeps: z.literal(1),
      retries: z.literal(1),
      root_effect: z.literal(1),
    }),
  }),
  journal: z
    .array(z.object({ hash: z.string(), created_at: z.string() }))
    .length(12),
});
const newResult = z.object({
  exit: z.literal(0),
  stage: z.literal('complete'),
  checks: z.record(z.string(), z.unknown()),
});

it.skipIf(!adminUrl || !pulsify || !target || !container || !receipt)(
  'drains real retained v4 work before migrating and reading every terminal history',
  async () => {
    if (!adminUrl || !pulsify || !target || !container || !receipt)
      throw new Error('Missing explicit native history fixture configuration');
    const adminAddress = new URL(adminUrl);
    if (!['127.0.0.1', 'localhost', '::1'].includes(adminAddress.hostname))
      throw new Error('Native history fixture must use loopback PostgreSQL');
    await mkdir(receipt, { recursive: true });
    const source = resolve('../..');
    const db = `workflow4a_${randomBytes(5).toString('hex')}`;
    const admin = new Pool({ connectionString: adminUrl });
    let pool: Pool | undefined;
    const writeLog = async (name: string, stdout: string, stderr: string) => {
      await writeFile(join(receipt, `${name}.log`), stdout + stderr);
    };
    try {
      await admin.query(`CREATE DATABASE "${db}"`);
      const dbUrl = new URL(adminUrl);
      dbUrl.pathname = `/${db}`;
      pool = new Pool({ connectionString: dbUrl.toString() });
      const env = {
        ...process.env,
        TZ: 'UTC',
        WORKFLOW_TARGET_WORLD: '@workflow/world-postgres',
        WORKFLOW_POSTGRES_URL: dbUrl.toString(),
        WORKFLOW_SLICE4A_URL: dbUrl.toString(),
        WORKFLOW_SLICE4A_SOURCE: source,
      };
      const oldBin = join(
        pulsify,
        'apps/api/node_modules/@workflow/world-postgres/bin/setup.js'
      );
      const oldBootstrap = await exec(process.execPath, [oldBin], { env });
      await writeLog('old-bootstrap', oldBootstrap.stdout, oldBootstrap.stderr);
      const initial = await pool.query(
        'SELECT hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      expect(initial.rows).toHaveLength(12);
      expect(await inspectWorkflowDatabase(pool)).toEqual({
        lineage: 'v4.3.10',
        pendingMigrations: 15,
      });
      const fixture = resolve('src/__fixtures__/migration-history-v4.mjs');
      const runPhase = async (phase: 'start' | 'finish') => {
        const child = spawn(process.execPath, [fixture], {
          env: { ...env, WORKFLOW_SLICE4A_PHASE: phase },
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        const pid = child.pid;
        if (!pid) throw new Error('Fixture process failed to start');
        let stdout = '',
          stderr = '';
        child.stdout.setEncoding('utf8');
        child.stderr.setEncoding('utf8');
        child.stdout.on('data', (chunk: string) => {
          stdout += chunk;
        });
        child.stderr.on('data', (chunk: string) => {
          stderr += chunk;
        });
        const result = await new Promise<{
          code: number | null;
          signal: NodeJS.Signals | null;
        }>((resolve, reject) => {
          child.once('error', reject);
          child.once('close', (code, signal) => resolve({ code, signal }));
        });
        await writeLog(`v4-${phase}`, stdout, stderr);
        expect(result).toEqual({ code: 0, signal: null });
        return pid;
      };
      const firstPid = await runPhase('start');
      const partial = z
        .object({
          exit: z.literal(0),
          stage: z.literal('partial'),
          pid: z.number().int(),
          ids: z.record(z.string(), z.string()),
        })
        .parse(
          JSON.parse(await readFile(join(receipt, 'partial.json'), 'utf8'))
        );
      expect(partial.pid).toBe(firstPid);
      expect(() => process.kill(firstPid, 0)).toThrow();
      const sdkBridge = await import(
        pathToFileURL(
          join(pulsify, 'apps/api/scripts/workflow-major-cutover.ts')
        ).href
      );
      const snapshot = async () =>
        (
          await pool.query(`SELECT
        (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM workflow.workflow_runs r) AS runs,
        (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM workflow.workflow_events e) AS events,
        (SELECT jsonb_agg(to_jsonb(s) ORDER BY step_id) FROM workflow.workflow_steps s) AS steps,
        (SELECT jsonb_agg(to_jsonb(h) ORDER BY hook_id) FROM workflow.workflow_hooks h) AS hooks,
        (SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id) FROM workflow.workflow_waits w) AS waits,
        (SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM graphile_worker._private_jobs j) AS jobs,
        (SELECT jsonb_agg(to_jsonb(f) ORDER BY label) FROM public.slice4a_effects f) AS effects`)
        ).rows[0];
      const beforeDrain = await snapshot();
      const pending = await pool.query(
        "SELECT (SELECT count(*)::int FROM workflow.workflow_waits WHERE run_id=$1 AND status='waiting' AND resume_at>now()) AS sleeps, (SELECT count(*)::int FROM workflow.workflow_steps WHERE run_id=$2 AND status='pending' AND retry_after>now()) AS retries, (SELECT count FROM public.slice4a_effects WHERE label='root') AS root_effect, (SELECT count(*)::int FROM workflow.workflow_steps WHERE run_id=$3 AND step_name='step//slice4a//business' AND status='completed') AS completed_business_step",
        [partial.ids.sleep, partial.ids.retry, partial.ids.root]
      );
      expect(pending.rows[0]).toEqual({
        sleeps: 1,
        retries: 1,
        root_effect: 1,
        completed_business_step: 1,
      });
      const counts = await sdkBridge.readDrainSnapshot(pool);
      expect(counts.active).toBe(4);
      expect(counts.legacyJobs).toBeGreaterThan(0);
      expect(counts.activeIds).toEqual(
        expect.arrayContaining([
          partial.ids.root,
          partial.ids.child,
          partial.ids.sleep,
          partial.ids.retry,
        ])
      );
      let now = 0;
      await expect(
        sdkBridge.pollUntilDrained(() => sdkBridge.readDrainSnapshot(pool), {
          now: () => now,
          pause: async (ms: number) => {
            now += ms;
          },
          timeoutMs: 1000,
        })
      ).rejects.toThrow('drain expired');
      const after = await snapshot();
      expect(after).toEqual(beforeDrain);
      const bridge = {
        drain: {
          counts,
          unchanged: true,
          timeout: true,
          before: beforeDrain,
          after,
        },
        firstPid,
        firstExit: 0,
        pending: pending.rows[0],
      };
      await writeFile(
        join(receipt, 'bridge.json'),
        JSON.stringify(bridge, null, 2)
      );
      const secondPid = await runPhase('finish');
      expect(secondPid).not.toBe(firstPid);
      expect(() => process.kill(secondPid, 0)).toThrow();
      const oldData = oldResult.parse(
        JSON.parse(await readFile(join(receipt, 'old.json'), 'utf8'))
      );
      expect(oldData.restart.oldPid).toBe(firstPid);
      expect(oldData.restart.newPid).toBe(secondPid);
      expect(oldData.drain.after).toEqual(oldData.drain.before);
      expect(oldData.journal).toEqual(initial.rows);
      expect(oldData.effects).toEqual([
        { label: 'failure', count: 1 },
        { label: 'retry', count: 2 },
        { label: 'root', count: 1 },
      ]);
      const dump = execFileSync(
        'docker',
        [
          'exec',
          '-u',
          'postgres',
          container,
          'pg_dump',
          '--format=custom',
          '--no-owner',
          '--no-acl',
          db,
        ],
        { maxBuffer: 32_000_000 }
      );
      await writeFile(join(receipt, 'v4-history.dump'), dump);
      const historicalSql = `SELECT json_build_object(
        'runs',(SELECT json_agg(row_to_json(r) ORDER BY id) FROM (SELECT id,status,input,output,error,encode(input_cbor,'hex') input_cbor,encode(output_cbor,'hex') output_cbor,encode(error_cbor,'hex') error_cbor,spec_version FROM workflow.workflow_runs) r),
        'events',(SELECT json_agg(row_to_json(e) ORDER BY id) FROM (SELECT id,run_id,type,correlation_id,payload,encode(payload_cbor,'hex') payload_cbor,spec_version FROM workflow.workflow_events) e),
        'steps',(SELECT json_agg(row_to_json(s) ORDER BY step_id) FROM (SELECT run_id,step_id,step_name,status,input,output,error,encode(input_cbor,'hex') input_cbor,encode(output_cbor,'hex') output_cbor,encode(error_cbor,'hex') error_cbor,spec_version FROM workflow.workflow_steps) s),
        'hooks',(SELECT json_agg(row_to_json(h) ORDER BY hook_id) FROM (SELECT run_id,hook_id,token,metadata,encode(metadata_cbor,'hex') metadata_cbor,spec_version FROM workflow.workflow_hooks) h),
        'waits',(SELECT json_agg(row_to_json(w) ORDER BY wait_id) FROM (SELECT wait_id,run_id,status,spec_version FROM workflow.workflow_waits) w)) AS history`;
      const before = await pool.query(historicalSql);
      const sql = await readFile(
        resolve('src/drizzle/migrations/0015a_catch_up_v4_schema.sql')
      );
      expect(createHash('sha256').update(sql).digest('hex')).toBe(
        '159c3de7aa454aeb4e59347402546dcb27011f6e0bbaf12c793d406964e049fd'
      );
      await pool.end();
      pool = undefined;
      const migrate = await exec(process.execPath, [resolve('bin/setup.js')], {
        env,
      });
      pool = new Pool({ connectionString: dbUrl.toString() });
      await writeLog('v5-bootstrap', migrate.stdout, migrate.stderr);
      const updated = await pool.query(
        'SELECT hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      expect(updated.rows).toHaveLength(27);
      expect(updated.rows.slice(0, 12)).toEqual(initial.rows);
      expect(
        updated.rows.some(
          (row) =>
            row.hash ===
            '159c3de7aa454aeb4e59347402546dcb27011f6e0bbaf12c793d406964e049fd'
        )
      ).toBe(true);
      expect((await pool.query(historicalSql)).rows).toEqual(before.rows);
      const repeat = await exec(process.execPath, [resolve('bin/setup.js')], {
        env,
      });
      await writeLog('v5-repeat', repeat.stdout, repeat.stderr);
      expect(
        (
          await pool.query(
            'SELECT hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
          )
        ).rows
      ).toEqual(updated.rows);
      const targetRun = await exec(
        process.execPath,
        [resolve('src/__fixtures__/migration-history-v5.mjs')],
        { env, maxBuffer: 8_000_000, timeout: 30_000 }
      );
      await writeLog('v5-reader', targetRun.stdout, targetRun.stderr);
      const targetData = newResult.parse(
        JSON.parse(await readFile(join(receipt, 'new.json'), 'utf8'))
      );
      expect(Object.keys(targetData.checks)).toEqual([
        'root',
        'child',
        'sleep',
        'retry',
        'failure',
        'getMany_none',
        'getMany_all',
      ]);
      expect((await pool.query(historicalSql)).rows).toEqual(before.rows);
      await writeFile(
        join(receipt, 'assertions.json'),
        JSON.stringify(
          {
            exit: 0,
            database: db,
            old: oldData,
            processes: {
              firstPid,
              firstExit: 0,
              secondPid,
              secondExit: 0,
              firstDeadBeforeSecond: true,
            },
            bridge,
            target: targetData,
            initialJournal: initial.rows,
            updatedJournal: updated.rows,
            dumpSha256: createHash('sha256').update(dump).digest('hex'),
          },
          null,
          2
        )
      );
    } finally {
      await pool?.end();
      await admin.end();
    }
  },
  120_000
);
