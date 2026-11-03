import { readMigrationFiles } from 'drizzle-orm/migrator';
import type { Pool } from 'pg';

// Column signatures of the genuine 4.3.10 and repaired 5.1.0 schemas.
// They include types, nullability and defaults, so a partial or modified
// schema fails closed rather than being treated as the known checkpoint.
const baselineColumns = 'a14bbf51edb68d69aed6d0cce3e87e37';
const targetColumns = '2f899ef9fa3134e2a872765744e8281b';
// PG17 pg_dump/pg_restore moves text casts inside the index predicate array.
// Both signatures were verified against full schema dumps; all other shapes refuse.
const baselineCatalog = [
  '4bd5c169d663fd5caec89106692ba2e6',
  '895ea90b59e9d15287b106fc76c415c4',
];
const targetCatalog = [
  '31bc392cb38eddae1841d1c50f5641b7',
  '5e160dded4f768cfeaa064b31a59be17',
];

function same(left: unknown[], right: unknown[]) {
  return JSON.stringify(left) === JSON.stringify(right);
}

export type WorkflowDatabaseInspection = {
  lineage: 'empty' | 'v4.3.10' | 'v5.1.0';
  pendingMigrations: number;
};

/** Refuse gap journals before Drizzle creates or changes any schema objects. */
export async function checkMigrationBaseline(
  pool: Pool,
  migrationsFolder: string
): Promise<WorkflowDatabaseInspection> {
  const refuse = () => {
    throw new Error('Unsupported workflow migration baseline or schema');
  };
  const migrations = readMigrationFiles({ migrationsFolder });
  const state = await pool.query<{
    journal: string | null;
    objects: number;
  }>(`SELECT
    to_regclass('workflow_drizzle.workflow_migrations')::text AS journal,
    (SELECT count(*)::int FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname IN ('workflow', 'workflow_drizzle') AND c.relkind IN ('r','p','v','m','S'))
    + (SELECT count(*)::int FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
      WHERE n.nspname IN ('workflow', 'workflow_drizzle') AND t.typtype IN ('e','d'))
    + (SELECT count(*)::int FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname IN ('workflow', 'workflow_drizzle')) AS objects`);
  if (!state.rows[0].journal) {
    if (state.rows[0].objects === 0)
      return { lineage: 'empty', pendingMigrations: migrations.length };
    return refuse();
  }
  const journal = await pool.query<{ created_at: string; hash: string }>(
    'SELECT created_at, hash FROM workflow_drizzle.workflow_migrations ORDER BY id'
  );
  const applied = journal.rows.map((row) => ({
    when: Number(row.created_at),
    hash: row.hash,
  }));
  const all = migrations.map((entry) => ({
    when: entry.folderMillis,
    hash: entry.hash,
  }));
  const old = all.filter((_, index) => index < 11 || index === 15);
  const upgraded = all.filter((_, index) => index < 11 || index >= 15);
  const isBaseline = same(applied, old);
  const isComplete = same(applied, all) || same(applied, upgraded);
  if (!isBaseline && !isComplete) return refuse();

  const columns = await pool.query<{ signature: string }>(`SELECT
    md5(string_agg(concat_ws('|',table_name,column_name,data_type,udt_schema,udt_name,is_nullable,
      coalesce(column_default,''),coalesce(character_maximum_length::text,'')), E'\\n' ORDER BY table_name,column_name)) AS signature
    FROM information_schema.columns WHERE table_schema = 'workflow'`);
  if (
    columns.rows[0].signature !== (isBaseline ? baselineColumns : targetColumns)
  )
    return refuse();
  const catalog = await pool.query<{
    signature: string;
  }>(`SELECT md5(string_agg(item, E'\\n' ORDER BY item)) AS signature FROM (
    SELECT 'constraint:' || c.relname || ':' || con.conname || ':' || pg_get_constraintdef(con.oid) AS item
      FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'workflow'
    UNION ALL SELECT 'index:' || pg_get_indexdef(i.indexrelid) || ':' || i.indisvalid::text || ':' || i.indisready::text
      FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'workflow'
    UNION ALL SELECT 'enum:' || t.typname || ':' || string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
      FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
      JOIN pg_enum e ON e.enumtypid = t.oid WHERE n.nspname = 'workflow' GROUP BY t.typname
  ) catalog`);
  if (
    !(isBaseline ? baselineCatalog : targetCatalog).includes(
      catalog.rows[0].signature
    )
  )
    return refuse();
  const latest = applied.at(-1)?.when ?? 0;
  return {
    lineage: isBaseline ? 'v4.3.10' : 'v5.1.0',
    pendingMigrations: migrations.filter((entry) => entry.folderMillis > latest)
      .length,
  };
}
