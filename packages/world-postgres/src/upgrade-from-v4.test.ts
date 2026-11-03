import { execFile, execFileSync } from 'node:child_process';
import { appendFile, cp, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { expect, it } from 'vitest';
import { inspectWorkflowDatabase } from './cli.js';
import { checkMigrationBaseline } from './migration-preflight.js';

const oldBootstrap = process.env.WORKFLOW_V4_BOOTSTRAP;
const testUrl = process.env.WORKFLOW_MIGRATION_TEST_URL;
const old4311Bootstrap = process.env.WORKFLOW_V4311_BOOTSTRAP;
const dockerContainer = process.env.WORKFLOW_MIGRATION_DOCKER_CONTAINER;
const exec = promisify(execFile);

it.skipIf(!oldBootstrap || !testUrl)(
  'upgrades the real 4.3.10 checkpoint without losing its journal',
  async () => {
    if (!testUrl || !oldBootstrap)
      throw new Error('Missing migration test fixture');
    const admin = new Pool({ connectionString: testUrl });
    await admin.query('CREATE DATABASE workflow_migration_test');
    const url = new URL(testUrl);
    url.pathname = '/workflow_migration_test';
    const pool = new Pool({ connectionString: url.toString() });
    try {
      expect(await inspectWorkflowDatabase(pool)).toEqual({
        lineage: 'empty',
        pendingMigrations: 31,
      });
      await exec(process.execPath, [oldBootstrap], {
        env: { ...process.env, WORKFLOW_POSTGRES_URL: url.toString() },
      });
      await pool.query(`INSERT INTO workflow.workflow_runs (id, deployment_id, status, name, input, spec_version)
        VALUES ('wrun_fixture', 'postgres', 'completed', 'fixture', '{"value":42}', '3');
        INSERT INTO workflow.workflow_events (id, run_id, type, payload, spec_version)
        VALUES ('evnt_fixture', 'wrun_fixture', 'run_created', '{"example":true}', 3)`);
      const before = await pool.query(
        'SELECT hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      expect(await inspectWorkflowDatabase(pool)).toEqual({
        lineage: 'v4.3.10',
        pendingMigrations: 15,
      });
      await checkMigrationBaseline(pool, 'src/drizzle/migrations');
      await pool.query(
        "UPDATE workflow_drizzle.workflow_migrations SET hash = 'wrong-hash' WHERE created_at = 1762873019948"
      );
      const corrupted = await pool.query(
        'SELECT hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      await expect(
        checkMigrationBaseline(pool, 'src/drizzle/migrations')
      ).rejects.toThrow('Unsupported workflow migration baseline');
      const unchanged = await pool.query(
        'SELECT hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      expect(unchanged.rows).toEqual(corrupted.rows);
      await pool.query(
        'UPDATE workflow_drizzle.workflow_migrations SET hash = $1 WHERE created_at = 1762873019948',
        [before.rows[0].hash]
      );
      await pool.query(
        'ALTER TABLE workflow.workflow_runs ADD COLUMN attributes text'
      );
      await expect(
        checkMigrationBaseline(pool, 'src/drizzle/migrations')
      ).rejects.toThrow('Unsupported workflow migration baseline');
      await pool.query(
        'ALTER TABLE workflow.workflow_runs DROP COLUMN attributes'
      );
      await pool.query(
        'ALTER TABLE workflow.workflow_runs ALTER COLUMN name TYPE text'
      );
      await expect(
        checkMigrationBaseline(pool, 'src/drizzle/migrations')
      ).rejects.toThrow('Unsupported workflow migration baseline');
      await pool.query(
        'ALTER TABLE workflow.workflow_runs ALTER COLUMN name TYPE varchar'
      );
      await pool.query(
        "ALTER TABLE workflow.workflow_runs ADD CONSTRAINT unexpected_check CHECK (name <> '')"
      );
      await expect(
        checkMigrationBaseline(pool, 'src/drizzle/migrations')
      ).rejects.toThrow('Unsupported workflow migration baseline');
      await pool.query(
        'ALTER TABLE workflow.workflow_runs DROP CONSTRAINT unexpected_check'
      );
      await migrate(drizzle(pool), {
        migrationsFolder: 'src/drizzle/migrations',
        migrationsTable: 'workflow_migrations',
        migrationsSchema: 'workflow_drizzle',
      });
      const after = await pool.query(
        'SELECT hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      expect(after.rows.slice(0, 12)).toEqual(before.rows);
      expect(after.rows).toHaveLength(27);
      expect(await inspectWorkflowDatabase(pool)).toEqual({
        lineage: 'v5.1.0',
        pendingMigrations: 0,
      });
      await checkMigrationBaseline(pool, 'src/drizzle/migrations');
      const data =
        await pool.query(`SELECT r.id, r.input, e.id AS event_id, e.payload
        FROM workflow.workflow_runs r JOIN workflow.workflow_events e ON e.run_id = r.id`);
      expect(data.rows).toEqual([
        {
          id: 'wrun_fixture',
          input: { value: 42 },
          event_id: 'evnt_fixture',
          payload: { example: true },
        },
      ]);
    } finally {
      await pool.end();
      await admin.query('DROP DATABASE workflow_migration_test');
      await admin.end();
    }
  },
  120_000
);

it.skipIf(!old4311Bootstrap || !testUrl)(
  'refuses the actual 4.3.11 gap journal before changing it',
  async () => {
    if (!testUrl || !old4311Bootstrap)
      throw new Error('Missing migration test fixture');
    const admin = new Pool({ connectionString: testUrl });
    await admin.query('CREATE DATABASE workflow_migration_4311_test');
    const url = new URL(testUrl);
    url.pathname = '/workflow_migration_4311_test';
    const pool = new Pool({ connectionString: url.toString() });
    try {
      await exec(process.execPath, [old4311Bootstrap], {
        env: { ...process.env, WORKFLOW_POSTGRES_URL: url.toString() },
      });
      const before = await pool.query(
        'SELECT id, hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      await expect(
        checkMigrationBaseline(pool, 'src/drizzle/migrations')
      ).rejects.toThrow('Unsupported workflow migration baseline');
      const after = await pool.query(
        'SELECT id, hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      expect(after.rows).toEqual(before.rows);
    } finally {
      await pool.end();
      await admin.query('DROP DATABASE workflow_migration_4311_test');
      await admin.end();
    }
  },
  120_000
);

it.skipIf(!oldBootstrap || !testUrl || !dockerContainer)(
  'accepts the two PG17 dump/restore index spellings without accepting a different index',
  async () => {
    if (!oldBootstrap || !testUrl || !dockerContainer)
      throw new Error('Missing migration restore fixture');
    const admin = new Pool({ connectionString: testUrl });
    const names = [
      'workflow_restore_original',
      'workflow_restore_old',
      'workflow_restore_new',
    ];
    for (const name of names) await admin.query(`CREATE DATABASE ${name}`);
    const url = new URL(testUrl);
    const poolFor = (name: string) => {
      const u = new URL(url);
      u.pathname = `/${name}`;
      return new Pool({ connectionString: u.toString() });
    };
    const dump = (name: string) =>
      execFileSync(
        'docker',
        ['exec', dockerContainer, 'pg_dump', '-U', 'postgres', '-Fc', name],
        { maxBuffer: 20_000_000 }
      );
    const restore = (name: string, input: Buffer) =>
      execFileSync(
        'docker',
        [
          'exec',
          '-i',
          dockerContainer,
          'pg_restore',
          '-U',
          'postgres',
          '-d',
          name,
          '--no-owner',
          '--no-acl',
        ],
        { input }
      );
    const old = poolFor(names[1]);
    const target = poolFor(names[2]);
    try {
      const initial = new URL(url);
      initial.pathname = `/${names[0]}`;
      await exec(process.execPath, [oldBootstrap], {
        env: { ...process.env, WORKFLOW_POSTGRES_URL: initial.toString() },
      });
      restore(names[1], dump(names[0]));
      await checkMigrationBaseline(old, 'src/drizzle/migrations');
      await migrate(drizzle(old), {
        migrationsFolder: 'src/drizzle/migrations',
        migrationsTable: 'workflow_migrations',
        migrationsSchema: 'workflow_drizzle',
      });
      await checkMigrationBaseline(old, 'src/drizzle/migrations');
      restore(names[2], dump(names[1]));
      await checkMigrationBaseline(target, 'src/drizzle/migrations');
      const correct = await target.query<{ definition: string }>(
        "SELECT pg_get_indexdef('workflow.workflow_events_entity_creation_unique'::regclass) AS definition"
      );
      const invalid = [
        "CREATE UNIQUE INDEX workflow_events_entity_creation_unique ON workflow.workflow_events (run_id, correlation_id, type) WHERE type IN ('step_created','hook_created','wait_created')",
        "CREATE UNIQUE INDEX workflow_events_entity_creation_unique ON workflow.workflow_events (run_id, correlation_id, type) WHERE type IN ('step_created','hook_created','wait_created','attr_set','other')",
        "CREATE INDEX workflow_events_entity_creation_unique ON workflow.workflow_events (run_id, correlation_id, type) WHERE type IN ('step_created','hook_created','wait_created','attr_set')",
        "CREATE UNIQUE INDEX workflow_events_entity_creation_unique ON workflow.workflow_events (correlation_id, run_id, type) WHERE type IN ('step_created','hook_created','wait_created','attr_set')",
      ];
      for (const definition of invalid) {
        await target.query(
          'DROP INDEX workflow.workflow_events_entity_creation_unique'
        );
        await target.query(definition);
        await expect(
          checkMigrationBaseline(target, 'src/drizzle/migrations')
        ).rejects.toThrow('Unsupported workflow migration baseline');
        await target.query(
          'DROP INDEX workflow.workflow_events_entity_creation_unique'
        );
        await target.query(correct.rows[0].definition);
      }
      await checkMigrationBaseline(target, 'src/drizzle/migrations');
    } finally {
      await old.end();
      await target.end();
      for (const name of names) await admin.query(`DROP DATABASE ${name}`);
      await admin.end();
    }
  },
  120_000
);

it.skipIf(!oldBootstrap || !testUrl).each([
  { mode: 'later SQL error', code: '42P01' },
  { mode: 'lock timeout', code: '55P03' },
])(
  'rolls back the final catch-up and journal on $mode',
  async ({ mode, code }) => {
    if (!oldBootstrap || !testUrl)
      throw new Error('Missing migration test fixture');
    const admin = new Pool({ connectionString: testUrl });
    await admin.query('CREATE DATABASE workflow_migration_rollback_test');
    const url = new URL(testUrl);
    url.pathname = '/workflow_migration_rollback_test';
    const pool = new Pool({ connectionString: url.toString() });
    const temp = await mkdtemp(join(tmpdir(), 'workflow-migration-rollback-'));
    let lock: Awaited<ReturnType<Pool['connect']>> | undefined;
    try {
      await exec(process.execPath, [oldBootstrap], {
        env: { ...process.env, WORKFLOW_POSTGRES_URL: url.toString() },
      });
      await pool.query(`INSERT INTO workflow.workflow_runs (id, deployment_id, status, name, input, spec_version)
      VALUES ('wrun_rollback', 'postgres', 'completed', 'fixture', '{"value":42}', '3');
      INSERT INTO workflow.workflow_events (id, run_id, type, payload, spec_version)
      VALUES ('evnt_rollback', 'wrun_rollback', 'run_created', '{"example":true}', 3)`);
      const snapshot = async () =>
        (
          await pool.query(`SELECT
      (SELECT json_agg(row_to_json(j) ORDER BY id) FROM (SELECT id,hash,created_at FROM workflow_drizzle.workflow_migrations) j) AS journal,
      (SELECT json_agg(column_name ORDER BY column_name) FROM information_schema.columns WHERE table_schema='workflow' AND (table_name,column_name) IN (('workflow_runs','error_code'),('workflow_runs','attributes'),('workflow_hooks','is_system'))) AS catchup_columns,
      pg_get_indexdef('workflow.workflow_events_entity_creation_unique'::regclass) AS index_definition,
      to_regclass('workflow.workflow_invocations')::text AS invocations,
      (SELECT json_agg(row_to_json(r)) FROM (SELECT id,input,spec_version FROM workflow.workflow_runs) r) AS runs,
      (SELECT json_agg(row_to_json(e)) FROM (SELECT id,run_id,type,payload FROM workflow.workflow_events) e) AS events`)
        ).rows[0];
      const before = await snapshot();
      let folder = 'src/drizzle/migrations';
      if (mode === 'later SQL error') {
        await cp(folder, temp, { recursive: true });
        await appendFile(
          join(temp, '0029_add_run_job_prefix.sql'),
          '\n--> statement-breakpoint\nSELECT * FROM workflow.injected_missing_table;\n'
        );
        folder = temp;
      } else {
        lock = await pool.connect();
        await lock.query('BEGIN');
        await lock.query(
          'LOCK TABLE workflow.workflow_runs IN ACCESS SHARE MODE'
        );
      }
      await expect(
        migrate(drizzle(pool), {
          migrationsFolder: folder,
          migrationsTable: 'workflow_migrations',
          migrationsSchema: 'workflow_drizzle',
        })
      ).rejects.toMatchObject({ cause: { code } });
      if (lock) {
        await lock.query('ROLLBACK');
        lock.release();
        lock = undefined;
      }
      const after = await snapshot();
      expect(after).toEqual(before);
      expect(after.journal).toHaveLength(12);
      expect(after.catchup_columns).toBeNull();
      expect(after.invocations).toBeNull();
      expect(after.runs).toEqual([
        { id: 'wrun_rollback', input: { value: 42 }, spec_version: '3' },
      ]);
      expect(after.events).toEqual([
        {
          id: 'evnt_rollback',
          run_id: 'wrun_rollback',
          type: 'run_created',
          payload: { example: true },
        },
      ]);
    } finally {
      if (lock) {
        await lock.query('ROLLBACK');
        lock.release();
      }
      await pool.end();
      await admin.query('DROP DATABASE workflow_migration_rollback_test');
      await admin.end();
      await rm(temp, { recursive: true, force: true });
    }
  },
  30_000
);
