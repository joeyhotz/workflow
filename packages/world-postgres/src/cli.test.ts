import { afterEach, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  pool: vi.fn(),
  end: vi.fn().mockResolvedValue(undefined),
}));
vi.mock('pg', () => ({
  Pool: vi.fn(function Pool(options: unknown) {
    mocks.pool(options);
    return { end: mocks.end };
  }),
}));
vi.mock('dotenv', () => ({ config: vi.fn() }));
vi.mock('drizzle-orm/node-postgres', () => ({ drizzle: vi.fn() }));
vi.mock('./migration-preflight.js', () => ({
  checkMigrationBaseline: vi
    .fn()
    .mockRejectedValue(new Error('connection failed')),
}));

import { setupDatabase } from './cli.js';

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

it('bounds bootstrap connections and redacts the connection password', async () => {
  vi.stubEnv(
    'WORKFLOW_POSTGRES_URL',
    'postgres://user:secret-fixture@127.0.0.1:1/db'
  );
  const log = vi.spyOn(console, 'log').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(process, 'exit').mockImplementation(() => {
    throw new Error('exit');
  });
  await expect(setupDatabase()).rejects.toThrow('exit');
  expect(mocks.pool).toHaveBeenCalledWith({
    connectionString: 'postgres://user:secret-fixture@127.0.0.1:1/db',
    max: 1,
    connectionTimeoutMillis: 10_000,
  });
  expect(JSON.stringify(log.mock.calls)).not.toContain('secret-fixture');
});
