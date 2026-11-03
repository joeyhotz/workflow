import { Pool } from 'pg';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';

const pool = new Pool({
  connectionString: process.env.WORKFLOW_POSTGRES_URL,
  max: 1,
  connectionTimeoutMillis: 10000,
});
try {
  const settings = await pool.query(`SELECT pg_backend_pid() pid,
    current_setting('TimeZone') timezone,
    current_setting('workflow.legacy_timezone') legacy_timezone,
    current_setting('lock_timeout') lock_timeout,
    current_setting('statement_timeout') statement_timeout,
    current_setting('application_name') application_name`);
  console.log(JSON.stringify({ settings: settings.rows[0] }));
  await migrate(drizzle(pool), {
    migrationsFolder: process.env.SLICE5_MIGRATIONS_FOLDER,
    migrationsSchema: 'workflow_drizzle',
    migrationsTable: 'workflow_migrations',
  });
  console.log(JSON.stringify({ unexpectedSuccess: true }));
} catch (error) {
  const causes = [];
  for (let e = error; e; e = e.cause)
    causes.push({ name: e.name, message: e.message, code: e.code ?? null });
  console.log(JSON.stringify({ causes }));
} finally {
  await pool.end();
}
process.exitCode = 1;
