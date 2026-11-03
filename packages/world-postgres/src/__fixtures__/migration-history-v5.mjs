import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
const old = JSON.parse(
  await readFile(process.env.WORKFLOW_SLICE4A_RECEIPT + '/old.json')
);
const req = createRequire(
  process.env.WORKFLOW_SLICE4A_TARGET + '/package.json'
);
const workflowPath = req.resolve('workflow/api');
const corePath = createRequire(workflowPath).resolve('@workflow/core/runtime');
const { getRun } = await import(pathToFileURL(workflowPath));
const { hydrateWorkflowArguments, hydrateStepReturnValue } = await import(
  pathToFileURL(
    process.env.WORKFLOW_SLICE4A_TARGET +
      '/node_modules/@workflow/core/dist/serialization.js'
  )
);
const { setWorld } = await import(pathToFileURL(corePath));
const worldPath =
  process.env.WORKFLOW_SLICE4A_SOURCE +
  '/packages/world-postgres/dist/index.js';
const { createWorld } = await import(pathToFileURL(worldPath));
const url = process.env.WORKFLOW_SLICE4A_URL;
const world = createWorld({ connectionString: url, jobPrefix: 'slice4a_v5_' });
setWorld(world);
const out = {
  exit: 1,
  stage: 'initial',
  identities: { workflowPath, corePath, worldPath },
  checks: {},
};
try {
  for (const [name, id] of Object.entries(old.ids)) {
    out.stage = name;
    const prior = old.data[name],
      run = getRun(id);
    const status = await run.status,
      completedAt = await run.completedAt;
    assert.equal(status, prior.status);
    assert.equal(completedAt.toISOString(), prior.completedAt);
    let result, error;
    try {
      result = await run.returnValue;
    } catch (e) {
      error = {
        name: e.name,
        message: e.message,
        cause: e.cause
          ? {
              message: e.cause.message,
              code: e.cause.code,
              stack: e.cause.stack,
            }
          : null,
      };
    }
    if (name === 'failure') assert.deepEqual(error, prior.error);
    else assert.deepEqual(result, prior.result);
    const events = await world.events.list({
      runId: id,
      pagination: { limit: 100, sortOrder: 'asc' },
      resolveData: 'all',
    });
    const steps = await world.steps.list({
      runId: id,
      pagination: { limit: 100, sortOrder: 'asc' },
      resolveData: 'all',
    });
    const hooks = await world.hooks.list({
      runId: id,
      pagination: { limit: 100, sortOrder: 'asc' },
      resolveData: 'all',
    });
    assert.deepEqual(
      events.data.map((e) => [e.eventId, e.eventType, e.correlationId ?? null]),
      prior.events.map((e) => [e.eventId, e.eventType, e.correlationId ?? null])
    );
    assert.deepEqual(
      steps.data
        .map((s) => ({ stepId: s.stepId, status: s.status }))
        .sort((a, b) => a.stepId.localeCompare(b.stepId)),
      prior.steps.toSorted((a, b) => a.stepId.localeCompare(b.stepId))
    );
    let ordering;
    if (name === 'root') {
      const ascending = prior.steps.map((s) => s.stepId).toSorted();
      assert.equal(ascending.length, 2);
      assert.deepEqual(
        steps.data.map((s) => s.stepId),
        ascending
      );
      const descending = await world.steps.list({
        runId: id,
        pagination: { limit: 100, sortOrder: 'desc' },
        resolveData: 'none',
      });
      const defaults = await world.steps.list({
        runId: id,
        pagination: { limit: 100 },
        resolveData: 'none',
      });
      assert.deepEqual(
        descending.data.map((s) => s.stepId),
        ascending.toReversed()
      );
      assert.deepEqual(
        defaults.data.map((s) => s.stepId),
        ascending.toReversed()
      );
      assert.deepEqual(
        defaults.data.map((s) => s.stepId),
        prior.steps.map((s) => s.stepId)
      );
      const pages = {};
      for (const direction of ['asc', 'desc']) {
        const first = await world.steps.list({
          runId: id,
          pagination: { limit: 1, sortOrder: direction },
          resolveData: 'none',
        });
        assert.equal(first.hasMore, true);
        assert.equal(first.cursor, first.data[0].stepId);
        const second = await world.steps.list({
          runId: id,
          pagination: { limit: 1, sortOrder: direction, cursor: first.cursor },
          resolveData: 'none',
        });
        assert.equal(second.hasMore, false);
        const ids = [...first.data, ...second.data].map((s) => s.stepId);
        assert.deepEqual(
          ids,
          direction === 'asc' ? ascending : ascending.toReversed()
        );
        pages[direction] = ids;
      }
      ordering = {
        ascending,
        descending: descending.data.map((s) => s.stepId),
        default: defaults.data.map((s) => s.stepId),
        pages,
      };
    }
    assert.deepEqual(hooks.data, prior.hooks);
    const decodedSteps = await Promise.all(
      steps.data
        .filter((s) => s.output)
        .map(async (s) => ({
          stepId: s.stepId,
          result: await hydrateStepReturnValue(s.output, id, undefined),
        }))
    );
    if (name === 'root')
      assert.ok(
        decodedSteps.some(
          (s) => s.result?.label === 'root' && s.result?.count === 1
        )
      );
    if (name === 'retry')
      assert.ok(decodedSteps.some((s) => s.result?.attempt === 2));
    out.checks[name] = {
      status,
      completedAt,
      result,
      error,
      events: events.data.map((e) => ({
        eventId: e.eventId,
        eventType: e.eventType,
        correlationId: e.correlationId,
      })),
      steps: steps.data.map((s) => ({ stepId: s.stepId, status: s.status })),
      hooks: hooks.data,
      decodedSteps,
      ordering,
    };
  }
  out.stage = 'getMany';
  const order = [
    old.ids.failure,
    old.ids.root,
    'wrun_missing_slice4a',
    old.ids.child,
    old.ids.root,
  ];
  for (const mode of ['none', 'all']) {
    const found = await world.runs.getMany(order, { resolveData: mode });
    assert.deepEqual(
      found.map((x) => x?.runId ?? null),
      [old.ids.failure, old.ids.root, null, old.ids.child, old.ids.root]
    );
    if (mode === 'none') assert.equal(found[1].input, undefined);
    else
      assert.deepEqual(
        await hydrateWorkflowArguments(found[1].input, old.ids.root, undefined),
        []
      );
    out.checks['getMany_' + mode] = found.map((x) => x?.runId ?? null);
  }
  out.stage = 'complete';
  out.exit = 0;
} catch (e) {
  out.error = { message: e.message, stack: e.stack };
  console.error(e);
} finally {
  await world.close();
  await writeFile(
    process.env.WORKFLOW_SLICE4A_RECEIPT + '/new.json',
    JSON.stringify(out, null, 2)
  );
}
if (out.exit !== 0) process.exitCode = 1;
