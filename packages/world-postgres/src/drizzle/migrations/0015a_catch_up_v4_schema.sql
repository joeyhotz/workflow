-- Published 4.3.10 recorded 0015 without 0011–0014. Catch those up before
-- 0021 reads attributes. Refuse partial or incompatible schema shapes.
SET LOCAL lock_timeout = '10s';--> statement-breakpoint
DO $$
DECLARE
  missing_count integer;
  correct_count integer;
  index_definition text;
  old_index constant text := 'CREATE UNIQUE INDEX workflow_events_entity_creation_unique ON workflow.workflow_events USING btree (run_id, correlation_id, type) WHERE ((type)::text = ANY ((ARRAY[''step_created''::character varying, ''hook_created''::character varying, ''wait_created''::character varying])::text[]))';
  -- PG17 dump/restore moves each text cast inside the array.
  restored_old_index constant text := 'CREATE UNIQUE INDEX workflow_events_entity_creation_unique ON workflow.workflow_events USING btree (run_id, correlation_id, type) WHERE ((type)::text = ANY (ARRAY[(''step_created''::character varying)::text, (''hook_created''::character varying)::text, (''wait_created''::character varying)::text]))';
  new_index constant text := 'CREATE UNIQUE INDEX workflow_events_entity_creation_unique ON workflow.workflow_events USING btree (run_id, correlation_id, type) WHERE ((type)::text = ANY ((ARRAY[''step_created''::character varying, ''hook_created''::character varying, ''wait_created''::character varying, ''attr_set''::character varying])::text[]))';
BEGIN
  SELECT count(*) FILTER (WHERE c.column_name IS NULL),
         count(*) FILTER (WHERE
           (expected.name = 'error_code' AND c.data_type = 'character varying' AND c.character_maximum_length IS NULL AND c.is_nullable = 'YES' AND c.column_default IS NULL) OR
           (expected.name = 'is_system' AND c.data_type = 'boolean' AND c.is_nullable = 'YES' AND c.column_default = 'false') OR
           (expected.name = 'attributes' AND c.data_type = 'jsonb' AND c.is_nullable = 'NO' AND c.column_default = '''{}''::jsonb'))
  INTO missing_count, correct_count
  FROM (VALUES ('workflow_runs', 'error_code'), ('workflow_hooks', 'is_system'), ('workflow_runs', 'attributes')) expected(tbl, name)
  LEFT JOIN information_schema.columns c ON c.table_schema = 'workflow' AND c.table_name = expected.tbl AND c.column_name = expected.name;

  SELECT pg_get_indexdef(i.indexrelid) INTO index_definition
  FROM pg_index i
  WHERE i.indexrelid = to_regclass('workflow.workflow_events_entity_creation_unique')
    AND i.indisvalid AND i.indisready;

  IF missing_count = 3 AND index_definition IN (old_index, restored_old_index) THEN
    ALTER TABLE "workflow"."workflow_runs" ADD COLUMN "error_code" varchar;
    ALTER TABLE "workflow"."workflow_hooks" ADD COLUMN "is_system" boolean DEFAULT false;
    ALTER TABLE "workflow"."workflow_runs" ADD COLUMN "attributes" jsonb DEFAULT '{}'::jsonb NOT NULL;
    DROP INDEX "workflow"."workflow_events_entity_creation_unique";
    CREATE UNIQUE INDEX "workflow_events_entity_creation_unique"
      ON "workflow"."workflow_events" ("run_id", "correlation_id", "type")
      WHERE "type" IN ('step_created', 'hook_created', 'wait_created', 'attr_set');
  ELSIF correct_count = 3 AND index_definition = new_index THEN
    NULL;
  ELSE
    RAISE EXCEPTION 'Unsupported workflow schema shape for 0015a catch-up';
  END IF;
END $$;
