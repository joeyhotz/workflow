import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { Pool } from 'pg';
import { expect, it } from 'vitest';

const adminUrl = process.env.SLICE4C4_ADMIN_URL;
const receipts = process.env.SLICE4C4_RECEIPTS;
const target = process.env.SLICE4C4_TARGET;
const source = resolve('../..');
const cleanEnv = Object.fromEntries(
  Object.entries(process.env).filter(([key]) => !key.startsWith('WORKFLOW_'))
);

it.skipIf(!adminUrl || !receipts || !target)(
  'recovers real v5 terminal deliveries after post-commit exceptions and real terminal conflicts',
  async () => {
    if (!adminUrl || !receipts || !target)
      throw new Error('Missing native terminal fixture');
    if (!['127.0.0.1', 'localhost', '::1'].includes(new URL(adminUrl).hostname))
      throw new Error('Postgres must be loopback');
    await mkdir(receipts, { recursive: true });
    const admin = new Pool({ connectionString: adminUrl });
    const pools: Pool[] = [];
    const all: Record<string, unknown> = {};
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
      const result = await new Promise<{
        code: number | null;
        signal: NodeJS.Signals | null;
      }>((resolve, reject) => {
        child.once('error', reject);
        child.once('close', (code, signal) => resolve({ code, signal }));
      });
      await writeFile(join(receipts, `${name}.log`), stdout + stderr);
      await writeFile(
        join(receipts, `${name}.exit.json`),
        JSON.stringify(result)
      );
      expect(result).toEqual({ code: 0, signal: null });
    };
    try {
      for (const shape of ['inline', 'queued'] as const)
        for (const mode of ['publication', 'delivery'] as const) {
          const name = `${shape}_${mode}`;
          const db = `slice4c4_${randomBytes(5).toString('hex')}`;
          await admin.query(`CREATE DATABASE "${db}"`);
          const url = new URL(adminUrl);
          url.pathname = `/${db}`;
          const pool = new Pool({ connectionString: url.toString() });
          pools.push(pool);
          const env: NodeJS.ProcessEnv = {
            ...cleanEnv,
            TZ: 'UTC',
            WORKFLOW_SPEC_VERSION: '8',
            WORKFLOW_TRACE_MODE: 'continuous',
            WORKFLOW_INLINE_EARLY_ADVANCE: '0',
            WORKFLOW_POSTGRES_INVOKE: '0',
            WORKFLOW_POSTGRES_EXPERIMENTAL_START_HOOK: '0',
            WORKFLOW_SKIP_BOOT_RECOVERY: '1',
            WORKFLOW_POSTGRES_JOB_PREFIX: 'api_v5_',
            WORKFLOW_QUEUE_NAMESPACE: 'slice4c4',
            WORKFLOW_POSTGRES_URL: url.toString(),
            SLICE4C4_URL: url.toString(),
            SLICE4C4_SOURCE: source,
            SLICE4C4_TARGET: target,
            SLICE4C4_OUT: join(receipts, `${name}.json`),
            SLICE4C4_SHAPE: shape,
            SLICE4C4_MODE: mode,
          };
          await run(`${name}-bootstrap`, resolve('bin/setup.js'), env);
          await pool.query(
            'CREATE TABLE public.slice4c4_effects (label text PRIMARY KEY,count integer NOT NULL)'
          );
          await run(
            name,
            resolve('src/__fixtures__/wake-redelivery-v5.mjs'),
            env
          );
          const result = JSON.parse(
            await readFile(join(receipts, `${name}.json`), 'utf8')
          );
          expect(result.exit).toBe(0);
          expect(result.injections).toBe(1);
          expect(result.worldSpec).toBe(8);
          expect(result.run).toMatchObject({
            status: 'completed',
            spec_version: '8',
          });
          expect(result.jobs).toEqual([]);
          expect(
            result.deliveries.every(
              (delivery: { path: string }) =>
                delivery.path === '/.well-known/workflow/v1/flow'
            )
          ).toBe(true);
          if (shape === 'queued')
            expect(
              result.deliveries.some(
                (delivery: { body: { stepId?: string } }) =>
                  delivery.body.stepId === result.stepId
              )
            ).toBe(true);

          expect(result.server).toMatchObject({
            timezone: 'Etc/UTC',
            search_path: '"$user", public',
            version: expect.stringContaining('PostgreSQL 17.11'),
          });
          expect(result.settings).toMatchObject({
            spec: '8',
            trace: 'continuous',
            earlyAdvance: '0',
            invoke: '0',
            startHook: '0',
            skipRecovery: '1',
            jobPrefix: 'api_v5_',
            inlineLimit: 3,
          });
          expect(result.wakeCall).toMatchObject({
            name: '__slice4c4_wkf_workflow_workflow//slice4c4//main',
            data: { runId: result.runId, runSpecVersion: 8 },
          });
          expect(result.wakeCall.data).not.toHaveProperty('stepId');
          expect(result.wakeCall.data).not.toHaveProperty('runInput');
          expect(result.boundary.jobs).toEqual([
            expect.objectContaining({
              identifier: 'api_v5_flows',
              attempts: 1,
              locked_by: expect.any(String),
            }),
          ]);
          expect(
            result.worldEvents.filter(
              (e: { correlationId?: string; type: string }) =>
                e.correlationId === result.stepId &&
                ['step_completed', 'step_failed', 'step_retrying'].includes(
                  e.type
                )
            )
          ).toEqual([
            expect.objectContaining({
              id: result.boundary.terminal[0].id,
              type: 'step_completed',
            }),
          ]);
          expect(result.worldSteps).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                id: result.stepId,
                name: 'step//slice4c4//target',
                status: 'completed',
              }),
            ])
          );
          expect(result.boundary.run_status).toBe('running');
          expect(result.boundary.step_status).toBe('completed');
          expect(result.wakeCall.elapsedMs).toBeGreaterThanOrEqual(500);
          expect(result.after.terminal).toEqual(result.boundary.terminal);
          expect(result.after.output_cbor).toBe(result.boundary.output_cbor);
          expect(result.after.error_cbor).toBe(result.boundary.error_cbor);
          expect(result.after.terminal).toEqual([
            expect.objectContaining({ type: 'step_completed' }),
          ]);
          expect(result.result).toBe('slice4c4-stream');
          expect(result.effects).toEqual(
            (shape === 'queued'
              ? ['continuation', 's1', 's2', 's3', 'target']
              : ['continuation', 'target']
            ).map((label) => ({ label, count: 1 }))
          );
          if (shape === 'queued')
            expect(result.release).toEqual({
              siblings: 3,
              initialFlowSettled: true,
              remainingJobs: 1,
            });
          const failed = result.deliveries.filter(
            (d: { status: number }) => d.status >= 500
          );
          expect(failed).toHaveLength(1);
          expect(failed[0].status).toBe(mode === 'publication' ? 500 : 503);
          expect(failed[0].headers['x-vqs-message-attempt']).toBe('1');
          const retry = result.deliveries.find(
            (d: { headers: Record<string, string> }) =>
              d.headers['x-vqs-message-id'] ===
                failed[0].headers['x-vqs-message-id'] &&
              d.headers['x-vqs-message-attempt'] === '2'
          );
          expect(retry).toMatchObject({
            status: 200,
            job: { id: failed[0].job.id, attempts: 2 },
          });
          expect(failed[0].job).toMatchObject({
            identifier: 'api_v5_flows',
            attempts: 1,
            locked_by: expect.any(String),
          });
          expect(retry.job.last_error).toEqual(expect.any(String));
          if (mode === 'delivery') {
            expect(failed[0].headers['x-vqs-message-id']).toBe(
              result.wakeMessageId
            );
            expect(result.deliveryBoundary).toEqual({
              run_status: 'running',
              step_status: 'completed',
              output_cbor: result.boundary.output_cbor,
              terminal_count: 1,
            });
            expect(failed[0].job.id).not.toBe(result.boundary.jobs[0].id);
          } else {
            expect(failed[0].job.id).toBe(result.boundary.jobs[0].id);
            expect(failed[0].headers['x-vqs-message-id']).toBe(
              result.boundary.jobs[0].payload.messageId
            );
            if (shape === 'queued')
              expect(result.startConflict).toMatchObject({
                isEntityConflict: true,
              });
          }
          all[name] = { db, ...result };
        }
      await writeFile(
        join(receipts, 'assertions.json'),
        JSON.stringify({ exit: 0, cases: all }, null, 2)
      );
    } finally {
      await Promise.all(pools.map((pool) => pool.end()));
      await admin.end();
    }
  },
  600_000
);
