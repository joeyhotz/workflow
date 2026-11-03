import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { Pool } from 'pg';
import { expect, it } from 'vitest';

const adminUrl = process.env.SLICE4C3_ADMIN_URL;
const receipts = process.env.SLICE4C3_RECEIPTS;
const target = process.env.SLICE4C3_TARGET;
const source = resolve('../..');
const cleanEnv = Object.fromEntries(
  Object.entries(process.env).filter(([key]) => !key.startsWith('WORKFLOW_'))
);

it.skipIf(!adminUrl || !receipts || !target)(
  'recovers real v5 terminal deliveries after the PostgreSQL COMMIT reply is lost',
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
      const server = (
        await admin.query(
          "SELECT version(),current_setting('TimeZone') timezone,current_setting('search_path') search_path"
        )
      ).rows[0];
      expect(server.version).toContain('PostgreSQL 17.11');
      expect(server.timezone).toBe('Etc/UTC');
      expect(server.search_path).toBe('\"$user\", public');
      await writeFile(
        join(receipts, 'server.json'),
        JSON.stringify(server, null, 2)
      );
      for (const shape of ['inline', 'queued'] as const)
        for (const outcome of ['success', 'fatal', 'exhausted'] as const) {
          const name = `${shape}_${outcome}`;
          const db = `slice4c3_${randomBytes(5).toString('hex')}`;
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
            WORKFLOW_QUEUE_NAMESPACE: 'slice4c3',
            WORKFLOW_POSTGRES_URL: url.toString(),
            SLICE4C3_URL: url.toString(),
            SLICE4C3_SOURCE: source,
            SLICE4C3_TARGET: target,
            SLICE4C3_OUT: join(receipts, `${name}.json`),
            SLICE4C3_SHAPE: shape,
            SLICE4C3_OUTCOME: outcome,
          };
          await run(`${name}-bootstrap`, resolve('bin/setup.js'), env);
          await pool.query(
            'CREATE TABLE public.slice4c3_effects (label text PRIMARY KEY,count integer NOT NULL)'
          );
          await run(
            name,
            resolve('src/__fixtures__/terminal-network-loss-v5.mjs'),
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

          expect(result.boundary.jobs).toEqual([
            expect.objectContaining({
              identifier: 'api_v5_flows',
              attempts: 1,
              locked_by: expect.any(String),
            }),
          ]);
          expect(result.worldSteps).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                id: result.stepId,
                name: 'step//slice4c3//target',
                status: outcome === 'success' ? 'completed' : 'failed',
              }),
            ])
          );
          expect(
            result.worldEvents.filter(
              (event: { correlationId?: string; type: string }) =>
                event.correlationId === result.stepId &&
                ['step_completed', 'step_failed', 'step_retrying'].includes(
                  event.type
                )
            )
          ).toEqual([
            expect.objectContaining({
              id: result.after.terminal[0].id,
              type: outcome === 'success' ? 'step_completed' : 'step_failed',
            }),
          ]);
          expect(result.effects).toEqual(
            (shape === 'queued'
              ? ['continuation', 's1', 's2', 's3', 'target']
              : ['continuation', 'target']
            ).map((label) => ({ label, count: 1 }))
          );

          expect(result.boundary.run_status).toBe('running');
          expect(result.after.terminal).toEqual(result.boundary.terminal);
          expect(result.after.step_status).toBe(
            outcome === 'success' ? 'completed' : 'failed'
          );
          expect(result.after.terminal).toEqual([
            expect.objectContaining({
              type: outcome === 'success' ? 'step_completed' : 'step_failed',
            }),
          ]);
          expect(
            result.effects.find((r: { label: string }) => r.label === 'target')
              .count
          ).toBe(1);
          expect(
            result.effects.find(
              (r: { label: string }) => r.label === 'continuation'
            ).count
          ).toBe(1);
          expect(result.result.next).toEqual({
            accepted: outcome === 'success' ? 'success' : 'failure',
          });
          if (outcome === 'success')
            expect(result.result.observed).toEqual({
              kind: 'success',
              value: { fixed: 'slice4c3-success' },
            });
          else if (outcome === 'fatal')
            expect(result.result.observed).toEqual({
              kind: 'failure',
              name: 'FatalError',
              message: 'slice4c3 fatal',
              cause: null,
            });
          else
            expect(result.result.observed).toEqual({
              kind: 'failure',
              name: 'FatalError',
              message:
                'Step \"step//slice4c3//target\" failed after 0 retries: slice4c3 ordinary',
              cause: { name: 'Error', message: 'slice4c3 ordinary' },
            });
          if (shape === 'queued')
            expect(result.release).toEqual({
              siblings: 3,
              initialFlowSettled: true,
              remainingJobs: 1,
            });
          expect(result.proxy.cut).toBe(true);
          expect(result.poolEvents).toEqual([
            expect.objectContaining({
              name: 'Error',
              message: 'Connection terminated unexpectedly',
            }),
          ]);
          expect(result.clientError).toMatchObject({
            name: 'Error',
            message: 'Failed query: rollback\nparams: ',
            cause: {
              name: 'Error',
              message:
                'Client has encountered a connection error and is not queryable',
            },
          });
          expect(result.proxy.events).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                action: 'armed',
                pid: result.proxyWarm.pid,
              }),
              expect.objectContaining({
                type: 'C',
                tag: 'BEGIN',
                suppressed: false,
              }),
              expect.objectContaining({
                type: 'C',
                tag: 'COMMIT',
                pid: result.proxyWarm.pid,
                suppressed: true,
              }),
              expect.objectContaining({ type: 'Z', suppressed: true }),
            ])
          );
          expect(result.proxy.expectedPid).toBe(result.proxyWarm.pid);
          expect(
            result.proxy.events
              .filter((e: { suppressed?: boolean }) => e.suppressed)
              .map((e: { type: string; tag?: string }) => [
                e.type,
                e.tag ?? null,
              ])
          ).toEqual([
            ['C', 'COMMIT'],
            ['Z', null],
          ]);
          expect(result.proxyReconnect.pid).not.toBe(result.proxyWarm.pid);
          expect(JSON.stringify(result.clientError)).toMatch(
            /connection|terminat|queryable|closed|socket/i
          );
          const failed = result.deliveries.filter(
            (d: { status: number }) => d.status >= 500
          );
          expect(failed).toHaveLength(1);
          const redelivered = result.deliveries.find(
            (d: { headers: Record<string, string> }) =>
              d.headers['x-vqs-message-id'] ===
                failed[0].headers['x-vqs-message-id'] &&
              d.headers['x-vqs-message-attempt'] === '2'
          );
          expect(failed[0].headers['x-vqs-message-attempt']).toBe('1');
          expect(failed[0].job).toMatchObject({
            id: result.boundary.jobs[0].id,
            identifier: 'api_v5_flows',
            attempts: 1,
            locked_by: expect.any(String),
          });
          expect(redelivered).toMatchObject({
            status: 200,
            job: {
              id: failed[0].job.id,
              identifier: 'api_v5_flows',
              attempts: 2,
              locked_by: expect.any(String),
            },
          });
          if (shape === 'queued' && outcome !== 'exhausted')
            expect(result.startConflict).toMatchObject({
              isEntityConflict: true,
            });
          if (shape === 'queued' && outcome === 'exhausted')
            expect(result.retryTerminalConflict).toMatchObject({
              isEntityConflict: true,
            });
          all[name] = { db, ...result };
        }
      await writeFile(
        join(receipts, 'assertions.json'),
        JSON.stringify({ exit: 0, server, cases: all }, null, 2)
      );
    } finally {
      await Promise.all(pools.map((pool) => pool.end()));
      await admin.end();
    }
  },
  600_000
);
