import { spawn } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { expect, it } from 'vitest';

const mode = process.env.SLICE5_MODE;
const receipts = process.env.SLICE5_RECEIPTS;
const url = process.env.SLICE5_ADMIN_URL;
const container = process.env.SLICE5_CONTAINER;
const pulsify = process.env.SLICE5_PULSIFY;

it.skipIf(!mode || !receipts || !url || !container || !pulsify)(
  'exercises the bounded old-schema volume harness in smoke mode',
  async () => {
    expect([
      'smoke',
      'monitor',
      'seed-volume',
      'volume-rollback',
      'volume-success',
    ]).toContain(mode);
    if (!receipts || !url || !container || !pulsify)
      throw new Error('Missing explicit native fixture configuration');
    await mkdir(receipts, { recursive: true });
    const proc = spawn(
      process.execPath,
      [resolve('src/__fixtures__/migration-volume-native.mjs')],
      {
        env: process.env,
        stdio: ['ignore', 'pipe', 'pipe'],
      }
    );
    let out = '',
      err = '';
    proc.stdout.on('data', (v: Buffer) => {
      out += v.toString();
    });
    proc.stderr.on('data', (v: Buffer) => {
      err += v.toString();
    });
    const exit = await new Promise<{
      code: number | null;
      signal: NodeJS.Signals | null;
    }>((resolve, reject) => {
      proc.once('error', reject);
      proc.once('close', (code, signal) => resolve({ code, signal }));
    });
    await writeFile(resolve(receipts, 'fixture.log'), out + err);
    await writeFile(
      resolve(receipts, 'fixture.exit.json'),
      JSON.stringify(exit)
    );
    expect(exit).toEqual({ code: 0, signal: null });
    const result = JSON.parse(
      await readFile(resolve(receipts, `${mode}-result.json`), 'utf8')
    );
    expect(result.exit).toBe(0);
    expect(result.mode).toBe(mode);
    if (mode === 'volume-success') {
      expect(result.migration.exit).toBe(0);
      expect(result.noop.exit).toBe(0);
      expect(result.blockedReader.code).toBe('57014');
      expect(result.blocked.dwellMs).toBeLessThan(3000);
      expect(result.postCommitRead).toBe('ok');
      expect(result.comparisons).toEqual({
        restoredMatches5b2: true,
        strictV4: true,
        strictV5: true,
        journal27Preserves12: true,
        baselineDigests: true,
        targetChecks: true,
        noOp: true,
        blockedReader: true,
        actualSession: true,
        indexesValid: true,
        graphileUnchanged: true,
      });
      expect(result.lockSamples.length).toBeGreaterThan(0);
      expect(result.monitor.length).toBeGreaterThan(0);
      return;
    }
    if (mode === 'volume-rollback') {
      expect(result.worker.exit).toEqual({ code: 1, signal: null });
      expect(result.worker.lines[1].causes).toEqual(
        expect.arrayContaining([expect.objectContaining({ code: '42P01' })])
      );
      expect(result.comparisons).toEqual({
        restoredMatches5b2: true,
        raw: true,
        utc: true,
        ddl: true,
        indexFlags: true,
        manifest: true,
        journal: true,
        sequenceOnly12To26: true,
        strictPreflight: true,
        child42P01: true,
        noOwnedWorker: true,
      });
      expect(result.retainedDumpSample.retainedDump).toBeGreaterThan(0);
      expect(result.monitor.length).toBeGreaterThan(0);
      return;
    }
    const expected =
      mode === 'seed-volume'
        ? { runs: 103241, steps: 692666, events: 2469805 }
        : { runs: 1000, steps: 6720, events: 23964 };
    expect(result.preflight).toEqual({
      lineage: 'v4.3.10',
      pendingMigrations: 15,
    });
    expect(result.postSeedInspection).toEqual(result.preflight);
    expect(result.journal).toHaveLength(12);
    expect(result.counts).toEqual({
      runs: expected.runs,
      steps: expected.steps,
      events: expected.events,
      hooks: 2,
      jobs: 0,
      active: 0,
      orphan_events: 0,
      orphan_steps: 0,
    });
    expect(result.restoreComparison).toMatchObject({
      raw: true,
      utc: true,
      tableManifest: true,
      sequence: true,
      otherDdlAndIndexFlags: true,
    });
    expect(result.envelopeCheck).toEqual({
      eventType: 'run_created',
      workflowName: 'workflow//slice4a//root',
      paddingLength: 64,
    });
    if (mode === 'smoke' || mode === 'monitor') {
      expect(result.lowLimit).toMatchObject({
        threshold: 1,
        reaped: true,
        controlledExit: 1,
        sqlExit: { code: '57014' },
        exit: { code: null, signal: 'SIGTERM' },
      });
      expect(result.lowLimit.controllerReason.category).toBe('resource_limit');
      expect(
        JSON.parse(await readFile(result.lowLimit.reasonFile, 'utf8'))
      ).toEqual(result.lowLimit.controllerReason);
      expect(result.lowLimit.cancellationErrors).toEqual([]);
      if (mode === 'monitor') {
        expect(result.churn).toMatchObject({
          exit: { code: 0, signal: null },
          samples: 12,
        });
        expect(result.sampleError).toMatchObject({
          controlExit: { code: 1, signal: null },
          reaped: true,
          sqlExit: { code: '57014' },
          exit: { code: null, signal: 'SIGTERM' },
          controllerReason: { category: 'sampling_error', error: { code: 73 } },
        });
        expect(result.sampleError.controllerReason.error.stderr).toContain(
          'injected sampler failure'
        );
        expect(result.sampleError.cancellationErrors).toEqual([]);
        expect(
          JSON.parse(await readFile(result.sampleError.reasonFile, 'utf8'))
        ).toEqual(result.sampleError.controllerReason);
      }
    } else {
      expect(result.calibration.attempt).toBe(1);
      expect(
        Object.values(result.sizeRatios).every(
          (r) => typeof r === 'number' && r >= 0.8 && r <= 1.2
        )
      ).toBe(true);
      expect(result.dump.sha256).toMatch(/^[0-9a-f]{64}$/);
      expect(result.seedDatabaseDropped).toBe(true);
      expect(result.restoredInspection).toEqual(result.preflight);
    }
    expect(result.integrity).toEqual({
      missing_step: 0,
      missing_hook: 0,
      invalid_step_lifecycle: 0,
      invalid_run_lifecycle: 0,
    });
    expect(result.invalidIds).toBe(0);
    expect(result.payloadCoherence).toEqual({
      run: true,
      step: true,
      hooks: true,
    });
    expect(result.before.raw['workflow.workflow_runs'].count).toBe(
      expected.runs
    );
    expect(result.before.raw['workflow.workflow_steps'].count).toBe(
      expected.steps
    );
    expect(result.before.raw['workflow.workflow_events'].count).toBe(
      expected.events
    );
    expect(result.monitor.length).toBeGreaterThan(0);
  },
  2_000_000
);
