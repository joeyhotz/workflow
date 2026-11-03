import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { Pool } from 'pg';
import { expect, it } from 'vitest';
import { inspectWorkflowDatabase } from './cli.js';

const testUrl = process.env.WORKFLOW_MIGRATION_TEST_URL;
const oldBootstrap = process.env.WORKFLOW_V4_BOOTSTRAP;
const v5Bootstrap = process.env.WORKFLOW_V5_BOOTSTRAP;
const exec = promisify(execFile);

it.skipIf(!testUrl || !oldBootstrap || !v5Bootstrap)(
  'inspects the real empty and v4 checkpoint without changes',
  async () => {
    if (!testUrl || !oldBootstrap)
      throw new Error('Missing migration test fixture');
    const admin = new Pool({ connectionString: testUrl });
    await admin.query('CREATE DATABASE workflow_slice3b1_v4_test');
    const url = new URL(testUrl);
    url.pathname = '/workflow_slice3b1_v4_test';
    const pool = new Pool({ connectionString: url.toString() });
    try {
      expect(await inspectWorkflowDatabase(pool)).toEqual({
        lineage: 'empty',
        pendingMigrations: 12,
      });
      await exec(process.execPath, [oldBootstrap], {
        env: { ...process.env, WORKFLOW_POSTGRES_URL: url.toString() },
      });
      const before = await pool.query(
        'SELECT id, hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      expect(await inspectWorkflowDatabase(pool)).toEqual({
        lineage: 'v4.3.10',
        pendingMigrations: 0,
      });
      expect(await inspectWorkflowDatabase(pool)).toEqual({
        lineage: 'v4.3.10',
        pendingMigrations: 0,
      });
      expect(
        (
          await pool.query(
            'SELECT id, hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
          )
        ).rows
      ).toEqual(before.rows);
      if (!v5Bootstrap) throw new Error('Missing v5 bootstrap fixture');
      await exec(process.execPath, [v5Bootstrap], {
        env: { ...process.env, WORKFLOW_POSTGRES_URL: url.toString() },
      });
      const upgraded = await pool.query(
        'SELECT id, hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
      );
      await expect(inspectWorkflowDatabase(pool)).rejects.toThrow(
        'Unsupported workflow migration baseline or schema'
      );
      expect(
        (
          await pool.query(
            'SELECT id, hash, created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
          )
        ).rows
      ).toEqual(upgraded.rows);
    } finally {
      await pool.end();
      await admin.query('DROP DATABASE workflow_slice3b1_v4_test');
      await admin.end();
    }
  },
  120_000
);
