import { execFile, execFileSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { appendFile, cp, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import { Pool } from 'pg';
import { expect, it } from 'vitest';
import { inspectWorkflowDatabase } from './cli.js';

const adminUrl = process.env.WORKFLOW_TZ_ADMIN_URL;
const container = process.env.WORKFLOW_TZ_CONTAINER;
const receipt = process.env.WORKFLOW_TZ_RECEIPT;
const pulsify = process.env.WORKFLOW_TZ_PULSIFY;
const target = process.env.WORKFLOW_TZ_TARGET;
const dumpPath = '/tmp/workflow5-slice4a1-proof/v4-history.dump';
const exec = promisify(execFile);
const folder = resolve('src/drizzle/migrations');
const opts = {
  migrationsFolder: folder,
  migrationsSchema: 'workflow_drizzle',
  migrationsTable: 'workflow_migrations',
};
const snapshotSql = `SELECT jsonb_build_object(
  'runs',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM workflow.workflow_runs r),
  'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM workflow.workflow_events e),
  'steps',(SELECT jsonb_agg(to_jsonb(s) ORDER BY step_id) FROM workflow.workflow_steps s),
  'waits',(SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id) FROM workflow.workflow_waits w),
  'hooks',(SELECT jsonb_agg(to_jsonb(h) ORDER BY hook_id) FROM workflow.workflow_hooks h),
  'journal',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM workflow_drizzle.workflow_migrations j),
  'columns',(SELECT jsonb_agg(jsonb_build_array(table_name,column_name,data_type) ORDER BY table_name,column_name) FROM information_schema.columns WHERE table_schema='workflow'),
  'index',pg_get_indexdef('workflow.workflow_events_entity_creation_unique'::regclass)) snapshot`;
const sampleSql = `SELECT id, extract(epoch FROM created_at AT TIME ZONE 'UTC')::text created_epoch, extract(epoch FROM completed_at AT TIME ZONE 'UTC')::text completed_epoch FROM workflow.workflow_runs WHERE id='wrun_01M6GG9R8TXXGXMMQKC6MV4YCY'`;
const afterSql = `SELECT id, extract(epoch FROM created_at)::text created_epoch, extract(epoch FROM completed_at)::text completed_epoch FROM workflow.workflow_runs WHERE id='wrun_01M6GG9R8TXXGXMMQKC6MV4YCY'`;

it.skipIf(!adminUrl || !container || !receipt || !pulsify || !target)(
  'preserves UTC-written history with explicit legacy timezone, and bounds migration failures',
  async () => {
    if (!adminUrl || !container || !receipt || !pulsify || !target)
      throw new Error('Missing native fixture configuration');
    if (!['127.0.0.1', 'localhost', '::1'].includes(new URL(adminUrl).hostname))
      throw new Error('Only loopback PostgreSQL permitted');
    const dump = await readFile(dumpPath);
    expect(createHash('sha256').update(dump).digest('hex')).toBe(
      'ba1f0f72007b2dc08ce66389ea75711cb0e811688d82c54b4c2fa162f4d712e4'
    );
    const sql = await readFile(join(folder, '0015a_catch_up_v4_schema.sql'));
    expect(createHash('sha256').update(sql).digest('hex')).toBe(
      '159c3de7aa454aeb4e59347402546dcb27011f6e0bbaf12c793d406964e049fd'
    );
    const admin = new Pool({ connectionString: adminUrl });
    const pools = new Set<Pool>();
    const output: Record<string, unknown> = {};
    const create = async (lane: string) => {
      const db = `slice4b_${lane}_${randomBytes(4).toString('hex')}`;
      await admin.query(`CREATE DATABASE "${db}"`);
      execFileSync(
        'docker',
        [
          'exec',
          '-i',
          container,
          'pg_restore',
          '-U',
          'workflow',
          '-d',
          db,
          '--no-owner',
          '--no-acl',
          '--exit-on-error',
        ],
        { input: dump }
      );
      const url = new URL(adminUrl);
      url.pathname = `/${db}`;
      const pool = new Pool({
        connectionString: url.toString(),
        options: '-c TimeZone=UTC',
      });
      pools.add(pool);
      expect(await inspectWorkflowDatabase(pool)).toEqual({
        lineage: 'v4.3.10',
        pendingMigrations: 15,
      });
      await pool.query(
        "INSERT INTO workflow.workflow_events (id,run_id,type,created_at,spec_version) VALUES ('wevt_slice4b_synthetic_summer','wrun_01M6GG9R8TXXGXMMQKC6MV4YCY','run_started','2026-01-15 12:00:00',3),('wevt_slice4b_synthetic_winter','wrun_01M6GG9R8TXXGXMMQKC6MV4YCY','run_started','2026-07-15 12:00:00',3)"
      );
      return { db, url: url.toString(), pool };
    };
    // pg_dump changes only its random psql restrict token on each invocation.
    const captureDatabase = async (db: string, pool: Pool, label: string) => {
      const ddl = execFileSync(
        'docker',
        ['exec', container, 'pg_dump', '-U', 'workflow', '--schema-only', db],
        { encoding: 'utf8', maxBuffer: 32_000_000 }
      ).replace(
        /^\\(un)?restrict [A-Za-z0-9]+$/gm,
        (_line, un) => `\\${un || ''}restrict <TOKEN>`
      );
      await writeFile(join(receipt, `${label}.sql`), ddl);
      const tables = (
        await pool.query(`SELECT n.nspname schema, c.relname name
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relkind IN ('r','p') AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname <> 'information_schema' ORDER BY 1,2`)
      ).rows;
      const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
      const contents: Record<
        string,
        { count: string; rows: string; sha256: string }
      > = {};
      for (const table of tables) {
        const name = `${table.schema}.${table.name}`;
        const result = (
          await pool.query(`SELECT count(*)::text count,
          coalesce(string_agg(to_jsonb(t)::text, E'\\n' ORDER BY to_jsonb(t)::text),'') rows
          FROM ${quote(table.schema)}.${quote(table.name)} t`)
        ).rows[0];
        contents[name] = {
          ...result,
          sha256: createHash('sha256').update(result.rows).digest('hex'),
        };
      }
      const indexes = (
        await pool.query(`SELECT n.nspname schema, c.relname name,
        i.indisvalid, i.indisready, i.indislive FROM pg_index i
        JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema'
        ORDER BY 1,2`)
      ).rows;
      const names = (
        await pool.query(`SELECT n.nspname schema, c.relname name
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relkind='S' AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname <> 'information_schema' ORDER BY 1,2`)
      ).rows;
      const sequences: Record<
        string,
        { last_value: string; is_called: boolean }
      > = {};
      for (const item of names) {
        sequences[`${item.schema}.${item.name}`] = (
          await pool.query(
            `SELECT last_value::text, is_called FROM ${quote(item.schema)}.${quote(item.name)}`
          )
        ).rows[0];
      }
      const state = {
        ddlSha256: createHash('sha256').update(ddl).digest('hex'),
        contents,
        indexes,
        sequences,
      };
      await writeFile(
        join(receipt, `${label}.json`),
        JSON.stringify(state, null, 2)
      );
      return { ddl, ...state };
    };
    const assertRollback = (
      beforeState: Awaited<ReturnType<typeof captureDatabase>>,
      afterState: Awaited<ReturnType<typeof captureDatabase>>,
      expectedLast: string
    ) => {
      expect(afterState.ddl).toBe(beforeState.ddl);
      expect(afterState.contents).toEqual(beforeState.contents);
      expect(afterState.indexes).toEqual(beforeState.indexes);
      const changed = Object.keys(beforeState.sequences).filter(
        (name) =>
          JSON.stringify(beforeState.sequences[name]) !==
          JSON.stringify(afterState.sequences[name])
      );
      expect(Object.keys(afterState.sequences)).toEqual(
        Object.keys(beforeState.sequences)
      );
      expect(changed).toEqual(['workflow_drizzle.workflow_migrations_id_seq']);
      expect(beforeState.sequences[changed[0]]).toEqual({
        last_value: '12',
        is_called: true,
      });
      expect(afterState.sequences[changed[0]]).toEqual({
        last_value: expectedLast,
        is_called: true,
      });
      return changed.map((name) => ({
        name,
        before: beforeState.sequences[name],
        after: afterState.sequences[name],
      }));
    };
    const sdkRead = async (
      url: string,
      lane: 'old' | 'new',
      zone: string,
      nodeZone: string
    ) => {
      const result = await exec(
        process.execPath,
        [resolve('src/__fixtures__/migration-timezone-reader.mjs')],
        {
          env: {
            ...process.env,
            WORKFLOW_POSTGRES_URL: url,
            WORKFLOW_TZ_LANE: lane,
            WORKFLOW_TZ_SOURCE: resolve('../..'),
            WORKFLOW_TZ_PULSIFY: pulsify,
            WORKFLOW_TZ_TARGET: target,
            TZ: nodeZone,
            PGOPTIONS: `-c TimeZone=${zone}`,
          },
          timeout: 30_000,
        }
      );
      return JSON.parse(result.stdout);
    };
    try {
      const origin = await create('utc');
      const before = (await origin.pool.query(sampleSql)).rows[0];
      const timestampColumns = (
        await origin.pool.query(
          "SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='workflow' AND data_type='timestamp without time zone' ORDER BY 1,2"
        )
      ).rows;
      const allInstants = async (pool: Pool, target: boolean) => {
        const output: Record<string, unknown> = {};
        for (const {
          table_name: table,
          column_name: column,
        } of timestampColumns) {
          const expression = target
            ? `\"${column}\"`
            : `\"${column}\" AT TIME ZONE 'UTC'`;
          const rows = await pool.query(
            `SELECT coalesce(to_jsonb(t)->>'id',to_jsonb(t)->>'step_id',to_jsonb(t)->>'hook_id',to_jsonb(t)->>'wait_id') id, extract(epoch FROM ${expression})::text epoch FROM workflow.\"${table}\" t ORDER BY 1`
          );
          output[`${table}.${column}`] = rows.rows;
        }
        return output;
      };
      const originalInstants = await allInstants(origin.pool, false);
      output.originalInstants = originalInstants;
      const initialJournal = (
        await origin.pool.query(
          'SELECT hash,created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
        )
      ).rows;
      expect(initialJournal).toHaveLength(12);
      output.server = (
        await admin.query(
          "SELECT version(),current_setting('TimeZone') timezone,current_setting('search_path') search_path"
        )
      ).rows[0];
      const oldReaders = [];
      for (const zone of ['UTC', 'Australia/Sydney'])
        for (const nodeZone of ['UTC', 'Australia/Sydney'])
          oldReaders.push(await sdkRead(origin.url, 'old', zone, nodeZone));
      output.oldReaders = oldReaders;
      for (const reader of oldReaders) {
        expect(reader.exit).toBe(0);
        expect(reader.settings.timezone).toBe(
          reader.pgOptions.replace('-c TimeZone=', '')
        );
        expect(reader.createdAt).toBe('2026-11-03T10:46:53.987Z');
        expect(reader.completedAt).toBe('2026-11-03T10:46:55.125Z');
        expect(reader.steps).toHaveLength(2);
        expect(reader.steps).toEqual(oldReaders[0].steps);
        expect(
          reader.events.filter(
            (event: { type: string }) => event.type === 'wait_created'
          )
        ).toMatchObject([
          { eventData: { resumeAt: '2026-11-03T10:47:24.072Z' } },
        ]);
      }
      const oldPids = oldReaders.map((reader) => reader.settings.pid);
      expect(new Set(oldPids).size).toBe(4);
      expect(
        (
          await admin.query(
            'SELECT pid FROM pg_stat_activity WHERE pid = ANY($1::int[])',
            [oldPids]
          )
        ).rows
      ).toEqual([]);
      const bridge = await import(
        pathToFileURL(
          join(pulsify, 'apps/api/scripts/workflow-major-cutover.ts')
        ).href
      );
      const monitor = await origin.pool.connect();
      const readerPool = new Pool({ connectionString: origin.url });
      try {
        const baseline = await bridge.countPrimarySessions(monitor, []);
        await readerPool.query('SELECT 1');
        const withReader = await bridge.countPrimarySessions(monitor, []);
        expect(withReader).toBe(baseline + 1);
        expect(() =>
          bridge.validateSessions(
            { visible: true, replica: false, sessions: withReader },
            false
          )
        ).toThrow('quiescence');
        await readerPool.end();
        expect(await bridge.countPrimarySessions(monitor, [])).toBe(baseline);
        output.sessionPolicy = { baseline, withReader, oldReaderBlocks: true };
      } finally {
        monitor.release();
      }
      const migrationResults = [];
      for (const [name, options, shift] of [
        ['utc', '-c TimeZone=UTC', 0],
        [
          'sydney_explicit',
          '-c TimeZone=Australia/Sydney -c workflow.legacy_timezone=UTC',
          0,
        ],
        ['sydney_negative', '-c TimeZone=Australia/Sydney', -39600],
      ] as const) {
        const fixture = name === 'utc' ? origin : await create(name);
        await fixture.pool.end();
        pools.delete(fixture.pool);
        const result = await exec(process.execPath, [resolve('bin/setup.js')], {
          env: {
            ...process.env,
            TZ: 'UTC',
            PGOPTIONS: options,
            WORKFLOW_POSTGRES_URL: fixture.url,
          },
          timeout: 30_000,
        });
        fixture.pool = new Pool({
          connectionString: fixture.url,
          options: '-c TimeZone=UTC',
        });
        pools.add(fixture.pool);
        await writeFile(
          join(receipt, `${name}-test-bootstrap.log`),
          result.stdout + result.stderr
        );
        const after = (await fixture.pool.query(afterSql)).rows[0];
        expect(Number(after.created_epoch) - Number(before.created_epoch)).toBe(
          shift
        );
        expect(after.completed_epoch).toBe(before.completed_epoch);
        const journal = (
          await fixture.pool.query(
            'SELECT hash,created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
          )
        ).rows;
        expect(journal).toHaveLength(27);
        expect(journal.slice(0, 12)).toEqual(initialJournal);
        const types = (
          await fixture.pool.query(
            "SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema='workflow' AND data_type LIKE '%timestamp%' ORDER BY 1,2"
          )
        ).rows;
        expect(
          types.every((row) => row.data_type === 'timestamp with time zone')
        ).toBe(true);
        const instants = await allInstants(fixture.pool, true);
        if (shift === 0) {
          expect(instants).toEqual(originalInstants);
          expect(await inspectWorkflowDatabase(fixture.pool)).toEqual({
            lineage: 'v5.1.0',
            pendingMigrations: 0,
          });
          const newReaders = [];
          for (const zone of ['UTC', 'Australia/Sydney'])
            for (const nodeZone of ['UTC', 'Australia/Sydney'])
              newReaders.push(
                await sdkRead(fixture.url, 'new', zone, nodeZone)
              );
          output[`${name}Readers`] = newReaders;
          for (const reader of newReaders) {
            expect(reader.exit).toBe(0);
            expect(reader.settings.timezone).toBe(
              reader.pgOptions.replace('-c TimeZone=', '')
            );
            expect(reader.prepared).toEqual([{ name: 'slice4b_target_plan' }]);
            expect(oldPids).not.toContain(reader.settings.pid);
            expect(reader.createdAt).toBe('2026-11-03T10:46:53.987Z');
            expect(reader.completedAt).toBe('2026-11-03T10:46:55.125Z');
            expect(reader.steps).toEqual(oldReaders[0].steps);
            expect(
              reader.events.filter(
                (event: { type: string }) => event.type === 'wait_created'
              )
            ).toMatchObject([
              { eventData: { resumeAt: '2026-11-03T10:47:24.072Z' } },
            ]);
          }
          const repeat = await exec(
            process.execPath,
            [resolve('bin/setup.js')],
            {
              env: {
                ...process.env,
                TZ: 'UTC',
                PGOPTIONS: options,
                WORKFLOW_POSTGRES_URL: fixture.url,
              },
              timeout: 30_000,
            }
          );
          await writeFile(
            join(receipt, `${name}-repeat.log`),
            repeat.stdout + repeat.stderr
          );
          expect(
            (
              await fixture.pool.query(
                'SELECT hash,created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
              )
            ).rows
          ).toEqual(journal);
        }
        const synthetic = (
          await fixture.pool.query(
            "SELECT id,extract(epoch FROM created_at)::text epoch FROM workflow.workflow_events WHERE id LIKE 'wevt_slice4b_synthetic_%' ORDER BY id"
          )
        ).rows;
        if (shift !== 0) {
          const originalSynthetic =
            originalInstants['workflow_events.created_at'];
          expect(originalSynthetic).toEqual(
            expect.arrayContaining([
              {
                id: 'wevt_slice4b_synthetic_summer',
                epoch: '1768478400.000000',
              },
              {
                id: 'wevt_slice4b_synthetic_winter',
                epoch: '1784116800.000000',
              },
            ])
          );
          expect(synthetic).toEqual([
            { id: 'wevt_slice4b_synthetic_summer', epoch: '1768438800.000000' },
            { id: 'wevt_slice4b_synthetic_winter', epoch: '1784080800.000000' },
          ]);
        }
        migrationResults.push({
          name,
          db: fixture.db,
          options,
          exit: 0,
          before,
          after,
          shift,
          journal,
          types,
          instants,
          synthetic,
        });
      }
      output.migrationResults = migrationResults;
      const unsafeReaders = [];
      for (const zone of ['UTC', 'Australia/Sydney']) {
        const unsafe = await create(
          zone === 'UTC' ? 'prepared_utc' : 'prepared_sydney'
        );
        const read = await exec(
          process.execPath,
          [resolve('src/__fixtures__/migration-timezone-reuse.mjs')],
          {
            env: {
              ...process.env,
              TZ: zone,
              PGOPTIONS: `-c TimeZone=${zone}`,
              WORKFLOW_POSTGRES_URL: unsafe.url,
              WORKFLOW_TZ_SOURCE: resolve('../..'),
              WORKFLOW_TZ_PULSIFY: pulsify,
            },
            timeout: 30_000,
          }
        );
        const result = JSON.parse(read.stdout);
        expect(result.exit).toBe(0);
        expect(result.settings.timezone).toBe(zone);
        expect(result.named).toMatchObject({ ok: false, code: '0A000' });
        expect(result.sdk).toMatchObject({
          ok: true,
          createdAt:
            zone === 'UTC'
              ? '2026-11-03T10:46:53.987Z'
              : '2026-11-03T21:46:53.987Z',
        });
        unsafeReaders.push({ db: unsafe.db, ...result });
      }
      output.unsafeReaders = unsafeReaders;
      const failed = await create('rollback');
      const prior = (await failed.pool.query(snapshotSql)).rows[0];
      const failedFullBefore = await captureDatabase(
        failed.db,
        failed.pool,
        'late-before'
      );
      const prepared = await failed.pool.connect();
      const query = {
        name: 'slice4b_before',
        text: 'SELECT created_at FROM workflow.workflow_runs WHERE id=$1',
        values: [before.id],
      };
      const pre = (await prepared.query(query)).rows;
      const temp = await mkdtemp(join(tmpdir(), 'workflow4b-'));
      try {
        await cp(folder, temp, { recursive: true });
        await appendFile(
          join(temp, '0029_add_run_job_prefix.sql'),
          `\n--> statement-breakpoint\nDO $$ BEGIN IF (SELECT data_type FROM information_schema.columns WHERE table_schema = 'workflow' AND table_name = 'workflow_runs' AND column_name = 'created_at') <> 'timestamp with time zone' THEN RAISE EXCEPTION '0027 did not convert timestamps'; END IF; END $$;\n--> statement-breakpoint\nSELECT * FROM workflow.slice4b_injected_missing;\n`
        );
        await expect(
          migrate(drizzle(failed.pool), { ...opts, migrationsFolder: temp })
        ).rejects.toMatchObject({ cause: { code: '42P01' } });
        expect((await prepared.query(query)).rows).toEqual(pre);
        expect((await failed.pool.query(snapshotSql)).rows[0]).toEqual(prior);
        const failedFullAfter = await captureDatabase(
          failed.db,
          failed.pool,
          'late-after'
        );
        const sequenceChanges = assertRollback(
          failedFullBefore,
          failedFullAfter,
          '26'
        );
        output.rollback = {
          full: { before: 'late-before', after: 'late-after', sequenceChanges },
          db: failed.db,
          sqlstate: '42P01',
          snapshot: prior,
          preparedStillUsable: true,
        };
      } finally {
        prepared.release();
        await import('node:fs/promises').then((fs) =>
          fs.rm(temp, { recursive: true })
        );
      }
      const locked = await create('locked');
      const lockBefore = (await locked.pool.query(snapshotSql)).rows[0];
      const lockFullBefore = await captureDatabase(
        locked.db,
        locked.pool,
        'lock-before'
      );
      const holder = await locked.pool.connect();
      await holder.query('BEGIN');
      await holder.query(
        'SELECT * FROM workflow.workflow_stream_chunks LIMIT 1'
      );
      const holderPid = (await holder.query('SELECT pg_backend_pid() pid'))
        .rows[0].pid;
      const startedAt = performance.now();
      const blocked = migrate(drizzle(locked.pool), opts);
      let observation;
      for (let i = 0; i < 80; i++) {
        const q = await admin.query(
          'SELECT pid,query,pg_blocking_pids(pid) blockers FROM pg_stat_activity WHERE datname=$1 AND $2=ANY(pg_blocking_pids(pid))',
          [locked.db, holderPid]
        );
        if (q.rows.length) {
          observation = q.rows;
          break;
        }
        await new Promise((r) => setTimeout(r, 100));
      }
      expect(observation).toHaveLength(1);
      expect(observation?.[0].query).toContain('-- Store every timestamp as');
      const concurrent = new Pool({
        connectionString: locked.url,
        statement_timeout: 1000,
      });
      try {
        await expect(
          concurrent.query(
            'SELECT * FROM workflow.workflow_stream_chunks LIMIT 1'
          )
        ).rejects.toMatchObject({ code: '57014' });
      } finally {
        await concurrent.end();
      }
      await expect(blocked).rejects.toMatchObject({ cause: { code: '55P03' } });
      const elapsedMs = performance.now() - startedAt;
      await holder.query('ROLLBACK');
      holder.release();
      expect((await locked.pool.query(snapshotSql)).rows[0]).toEqual(
        lockBefore
      );
      const lockFullAfter = await captureDatabase(
        locked.db,
        locked.pool,
        'lock-after'
      );
      const lockSequenceChanges = assertRollback(
        lockFullBefore,
        lockFullAfter,
        '24'
      );
      output.lock = {
        full: {
          before: 'lock-before',
          after: 'lock-after',
          sequenceChanges: lockSequenceChanges,
        },
        db: locked.db,
        holderPid,
        observation,
        elapsedMs,
        sqlstate: '55P03',
        concurrentReadSqlstate: '57014',
        snapshot: lockBefore,
      };
      await writeFile(
        join(receipt, 'native-assertions.json'),
        JSON.stringify({ exit: 0, ...output }, null, 2)
      );
    } finally {
      await Promise.all([...pools].map((pool) => pool.end()));
      await admin.end();
    }
  },
  180_000
);
