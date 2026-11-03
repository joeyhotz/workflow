import assert from 'node:assert/strict';
import { spawn, execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import {
  mkdir,
  statfs,
  writeFile,
  lstat,
  readdir,
  open,
  stat,
  readFile,
  cp,
  appendFile,
} from 'node:fs/promises';
import { dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';
import { cpus, totalmem, freemem } from 'node:os';
import { decode, encode } from 'cbor-x';
import { pipeline } from 'node:stream/promises';
import { promisify } from 'node:util';
import { Pool } from 'pg';
import { inspectWorkflowDatabase } from '../../dist/cli.js';

// Captured from the real v4 runtime histories in slice 4a1. These are storage
// envelopes, not claims that the synthetic histories were executed by an SDK.
const templates = {
  hook_created:
    'b9000365746f6b656e6d736c69636534612d6368696c64686d65746164617461f7696973576562686f6f6bf4',
  hook_received:
    'b9000265746f6b656e6d736c69636534612d6368696c64677061796c6f6164d840581c6465766c5b7b226163636570746564223a317d2c226368696c64225d',
  run_completed:
    'b90001666f7574707574d84058286465766c5b7b226368696c64223a317d2c7b226163636570746564223a327d2c226368696c64225d',
  run_created:
    'b900046c6465706c6f796d656e74496468706f7374677265736c776f726b666c6f774e616d6577776f726b666c6f772f2f736c69636534612f2f726f6f7465696e707574d840486465766c5b5b5d5d70657865637574696f6e436f6e74657874b900036c747261636543617272696572b9000073776f726b666c6f77436f726556657273696f6e66342e382e3135686665617475726573b900016a656e6372797074696f6ef4',
  run_failed:
    'b90002656572726f72b90002676d65737361676576736c6963653461206c6567616379206661696c75726565737461636b79093a4572726f723a20736c6963653461206c6567616379206661696c7572650a20202020617420736c69636534613a363a3130380a2020202061742070726f636573732e70726f636573735469636b73416e6452656a656374696f6e7320286e6f64653a696e7465726e616c2f70726f636573732f7461736b5f7175657565733a3130343a35290a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f776f726b666c6f772e6a733a3634303a32380a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f74656c656d657472792e6a733a37393a32380a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f72756e74696d652e6a733a3532383a33360a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f74656c656d657472792e6a733a37393a32380a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f72756e74696d652e6a733a3532343a34320a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f74656c656d657472792e6a733a37393a32380a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f72756e74696d652e6a733a3137383a32300a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f74656c656d657472792e6a733a35303a3630696572726f72436f64656a555345525f4552524f52',
  step_completed:
    'b9000368737465704e616d6577737465702f2f736c69636534612f2f627573696e6573736c776f726b666c6f774e616d6577776f726b666c6f772f2f736c69636534612f2f726f6f7466726573756c74d84058246465766c5b7b226c6162656c223a312c22636f756e74223a327d2c22726f6f74222c315d',
  step_created:
    'b9000368737465704e616d6577737465702f2f736c69636534612f2f627573696e6573736c776f726b666c6f774e616d6577776f726b666c6f772f2f736c69636534612f2f726f6f7465696e707574d84058396465766c5b7b2261726773223a312c22636c6f7375726556617273223a2d312c227468697356616c223a2d317d2c5b325d2c22726f6f74225d',
  step_retrying:
    'b9000468737465704e616d6574737465702f2f736c69636534612f2f7265747279656572726f726d736c696365346120726574727965737461636b7909b1526574727961626c654572726f723a20736c69636534612072657472790a2020202061742066696c653a2f2f2f55736572732f6a6f6579686f747a2f6465762f776f726b74726565732f776f726b666c6f772d76352d736166652d6d6967726174696f6e2f7061636b616765732f776f726c642d706f7374677265732f7372632f5f5f66697874757265735f5f2f6d6967726174696f6e2d686973746f72792d76342e6d6a733a3130383a31330a2020202061742070726f636573732e70726f636573735469636b73416e6452656a656374696f6e7320286e6f64653a696e7465726e616c2f70726f636573732f7461736b5f7175657565733a3130343a35290a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f72756e74696d652f737465702d68616e646c65722e6a733a3432303a32380a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f74656c656d657472792e6a733a37393a32380a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f72756e74696d652f737465702d68616e646c65722e6a733a3431393a32360a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f74656c656d657472792e6a733a37393a32380a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f74656c656d657472792e6a733a35303a36300a202020206174206173796e63207374657048616e646c6572202866696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f72756e74696d652f737465702d68616e646c65722e6a733a3131333a3132290a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b776f726c642d6c6f63616c40342e342e335f406f70656e74656c656d657472792b61706940312e392e312f6e6f64655f6d6f64756c65732f40776f726b666c6f772f776f726c642d6c6f63616c2f646973742f71756575652e6a733a3238353a33320a202020206174206173796e632066696c653a2f2f2f55736572732f6a6f6579686f747a2f2e706173656f2f776f726b74726565732f326b62386a697a6d2f776f726b666c6f77352d736166652d6d6967726174696f6e2f6e6f64655f6d6f64756c65732f2e706e706d2f40776f726b666c6f772b636f726540342e382e31355f70617463685f686173683d3864386166363761306632633534633862326163656637343665643239386162623037633530663032613461613263393163666432615f66303662343465616165333636366338333436306639633134656463376332632f6e6f64655f6d6f64756c65732f40776f726b666c6f772f636f72652f646973742f72756e74696d652f68656c706572732e6a733a3532323a31366a72657472794166746572c1fb41daba6eef06147b',
  step_started:
    'b9000168737465704e616d6577737465702f2f736c69636534612f2f627573696e657373',
};
// Re-encode observed envelope shapes with coherent synthetic values. The row
// fields and their corresponding lifecycle-event payloads use the same values.
const syntheticError = {
  message: 'synthetic failure',
  stack: 'Error: synthetic failure',
};
const decodeTemplate = (name) => decode(Buffer.from(templates[name], 'hex'));
const failed = decodeTemplate('run_failed');
failed.error = syntheticError;
templates.run_failed = Buffer.from(encode(failed)).toString('hex');
const retry = decodeTemplate('step_retrying');
retry.stepName = 'step//slice4a//business';
retry.error = 'synthetic retry';
retry.stack = 'Error: synthetic retry';
delete retry.retryAfter;
templates.step_retrying = Buffer.from(encode(retry)).toString('hex');
for (const kind of ['hook_created', 'hook_received'])
  for (let i = 1; i <= 2; i++) {
    const v = decodeTemplate(kind);
    v.token = 'token_' + i;
    templates[kind + '_' + i] = Buffer.from(encode(v)).toString('hex');
  }
const runInput = Buffer.from(
  encode(decodeTemplate('run_created').input)
).toString('hex');
const runOutput = Buffer.from(
  encode(decodeTemplate('run_completed').output)
).toString('hex');
const runContext = Buffer.from(
  encode(decodeTemplate('run_created').executionContext)
).toString('hex');
const stepInput = Buffer.from(
  encode(decodeTemplate('step_created').input)
).toString('hex');
const stepOutput = Buffer.from(
  encode(decodeTemplate('step_completed').result)
).toString('hex');
const SMOKE = {
  runs: 1000,
  seven: 720,
  steps: 6720,
  retries: 400,
  hooks: 2,
  events: 23964,
};
const FULL = {
  runs: 103241,
  seven: 73220,
  steps: 692666,
  retries: 41040,
  hooks: 2,
  events: 2469805,
};
const LIMITS = {
  owned: 10 * 1024 ** 3,
  free: 20 * 1024 ** 3,
  preparationMs: 30 * 60e3,
  migrationMs: 30 * 60e3,
  totalMs: 90 * 60e3,
};
const exec = promisify(execFile);
const q = (s) => '"' + s.replaceAll('"', '""') + '"';
const env = process.env;
const container = env.SLICE5_CONTAINER;
const output = env.SLICE5_RECEIPTS;
const adminUrl = env.SLICE5_ADMIN_URL;
const pulsify = env.SLICE5_PULSIFY;
const fixtureMode = env.SLICE5_MODE;
assert.ok(
  [
    'smoke',
    'monitor',
    'low-control',
    'sample-error-control',
    'seed-volume',
    'volume-rollback',
    'volume-success',
  ].includes(fixtureMode),
  'Explicit smoke, seed-volume, or internal low-limit control required'
);
assert.ok(container && output && adminUrl && pulsify);
assert.ok(
  ['127.0.0.1', 'localhost', '::1'].includes(new URL(adminUrl).hostname)
);
await mkdir(output, { recursive: true });
const started = Date.now();
let migrationStarted;
let migrationElapsedMs = 0;
const outputJson = {
  mode: fixtureMode,
  cardinality: fixtureMode === 'seed-volume' ? FULL : SMOKE,
  fullRecipe: FULL,
  limits: LIMITS,
  pid: process.pid,
  monitor: [],
  childExits: [],
  exit: 1,
};
const children = new Set();
let currentPid;
let boundError;
let migrationAbort;
const command = async (file, args, opts = {}) =>
  exec(file, args, { maxBuffer: 8 * 1024 * 1024, timeout: 20_000, ...opts });
function child(file, args, opts = {}) {
  const proc = spawn(file, args, opts);
  children.add(proc);
  const done = new Promise((resolve, reject) => {
    proc.once('error', reject);
    proc.once('close', (code, signal) => {
      children.delete(proc);
      outputJson.childExits.push({ file, args, pid: proc.pid, code, signal });
      resolve({ code, signal });
    });
  });
  return { proc, done };
}
async function run(file, args, options = {}) {
  const { proc, done } = child(file, args, {
    ...options,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '',
    stderr = '';
  proc.stdout.on('data', (c) => (stdout += c));
  proc.stderr.on('data', (c) => (stderr += c));
  const result = await done;
  if (result.code !== 0)
    throw new Error(
      `${file} exited ${result.code}/${result.signal}: ${stderr}`
    );
  return { stdout, stderr };
}
const docker = (...args) => run('docker', ['exec', container, ...args]);
// GNU find's -ignore_readdir_race suppresses only an entry disappearing after
// readdir; permission and I/O failures retain a nonzero exit (pipefail).
// Both traversals count allocated blocks, including directories and symlinks.
// Samples are not atomic: high-water marks plus a 256 MiB reserve tighten the
// 10 GiB limit, but cannot bound arbitrary growth between one-second samples.
const reserveBytes = 256 * 1024 ** 2;
let peakPgdata = 0,
  peakReceipt = 0;
async function receiptBlocks(path, root = true) {
  let st;
  try {
    st = await lstat(path);
  } catch (e) {
    if (!root && e.code === 'ENOENT') return 0;
    throw e;
  }
  let total = st.blocks * 512;
  if (st.isDirectory()) {
    let names;
    try {
      names = await readdir(path);
    } catch (e) {
      if (!root && e.code === 'ENOENT') return total;
      throw e;
    }
    for (const name of names)
      total += await receiptBlocks(path + '/' + name, false);
  }
  if (!Number.isSafeInteger(total) || total < 0)
    throw new Error('invalid receipt allocated bytes');
  return total;
}
function describeError(e) {
  return {
    name: e?.name ?? null,
    message: e?.message ?? String(e),
    command: e?.cmd ?? null,
    code: e?.code ?? null,
    signal: e?.signal ?? null,
    stderr: e?.stderr ?? null,
    syscall: e?.syscall ?? null,
    path: e?.path ?? null,
  };
}
async function usage(injectFailure = false) {
  const args = injectFailure
    ? [
        'exec',
        container,
        'bash',
        '-c',
        'printf "injected sampler failure\n" >&2; exit 73',
      ]
    : [
        'exec',
        container,
        'bash',
        '-o',
        'pipefail',
        '-c',
        String.raw`find "$1" -ignore_readdir_race -xdev -printf "%b\n" | awk '{s+=$1} END {printf "%.0f\n", s*512}'`,
        'slice5-find',
        '/var/lib/postgresql/data',
      ];
  const [db, receipt, fs, retained] = await Promise.all([
    command('docker', args),
    receiptBlocks(dirname(output)),
    statfs(output),
    ['volume-rollback', 'volume-success'].includes(fixtureMode)
      ? stat('/tmp/workflow5-slice5b2-proof/run/full-baseline.dump')
      : Promise.resolve(null),
  ]);
  const text = db.stdout.trim();
  if (!/^\d+$/.test(text)) throw new Error('invalid PGDATA sampler output');
  const pgdata = Number(text),
    free = Number(fs.bavail) * Number(fs.bsize);
  const retainedDump = retained ? retained.blocks * 512 : 0;
  for (const value of [pgdata, receipt, free, retainedDump])
    if (!Number.isSafeInteger(value) || value < 0)
      throw new Error('invalid resource sample');
  peakPgdata = Math.max(peakPgdata, pgdata);
  peakReceipt = Math.max(peakReceipt, receipt);
  return {
    at: Date.now(),
    pgdata,
    receipt,
    retainedDump,
    free,
    peakPgdata,
    peakReceipt,
    reserveBytes,
    guardedOwned: peakPgdata + peakReceipt + retainedDump + reserveBytes,
  };
}
let controller;
let firstCancellation;
let cancelPromise;
async function cancelOwned(reason) {
  if (cancelPromise) return cancelPromise;
  firstCancellation = reason;
  outputJson.cancelReason = reason;
  boundError = new Error('slice5 monitor ' + reason.category);
  cancelPromise = (async () => {
    let persistenceError;
    const path = output + '/cancellation-' + process.pid + '.json';
    try {
      const f = await open(path, 'wx');
      try {
        await f.writeFile(JSON.stringify(reason, null, 2));
        await f.sync();
      } finally {
        await f.close();
      }
      const dir = await open(output, 'r');
      try {
        await dir.sync();
      } finally {
        await dir.close();
      }
      outputJson.cancelReasonPath = path;
    } catch (e) {
      persistenceError = describeError(e);
      outputJson.reasonPersistenceError = persistenceError;
    }
    const errors = [];
    if (currentPid && controller)
      try {
        await controller.query('SELECT pg_cancel_backend($1)', [currentPid]);
      } catch (e) {
        errors.push(describeError(e));
      }
    if (migrationStarted && controller)
      try {
        await controller.query(
          'SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name=$1 AND pid<>pg_backend_pid()',
          ['slice5a-owned-migration-' + process.pid]
        );
      } catch (e) {
        errors.push(describeError(e));
      }
    migrationAbort?.abort(boundError);
    for (const p of children)
      if (!p.killed && !p.kill('SIGTERM'))
        errors.push({ message: 'Failed to signal owned child', pid: p.pid });
    if (errors.length) outputJson.cancellationErrors = errors;
    if (persistenceError || errors.length)
      boundError = new Error(
        'slice5 monitor cancellation or persistence failure'
      );
  })();
  return cancelPromise;
}
// The first cause is fsynced before cancellation or signals; later errors never
// replace it. Only internal test code can supply a low threshold/injection.
async function startMonitor(limits = LIMITS, injectFailure = false) {
  let stopped = false,
    running = false;
  const sample = async () => {
    if (stopped || running || firstCancellation) return;
    running = true;
    try {
      let u;
      try {
        u = await usage(injectFailure);
      } catch (e) {
        await cancelOwned({
          category: 'sampling_error',
          at: Date.now(),
          elapsedMs: Date.now() - started,
          limits,
          latestSample: outputJson.monitor.at(-1) ?? null,
          error: describeError(e),
        });
        return;
      }
      outputJson.monitor.push(u);
      const elapsedMs = Date.now() - started;
      const triggered = [];
      if (u.guardedOwned > limits.owned) triggered.push('owned_bytes');
      if (u.free < limits.free) triggered.push('free_bytes');
      if (
        !migrationStarted &&
        elapsedMs - migrationElapsedMs > limits.preparationMs
      )
        triggered.push('preparation_deadline');
      if (
        migrationStarted &&
        Date.now() - migrationStarted > limits.migrationMs
      )
        triggered.push('migration_deadline');
      if (elapsedMs > limits.totalMs) triggered.push('total_deadline');
      if (triggered.length)
        await cancelOwned({
          category: 'resource_limit',
          at: Date.now(),
          elapsedMs,
          limits,
          sample: u,
          triggered,
          error: null,
        });
    } finally {
      running = false;
    }
  };
  await sample();
  const timer = setInterval(() => {
    void sample().catch(async (e) => {
      outputJson.monitorUnexpected = describeError(e);
      await cancelOwned({
        category: 'monitor_internal_error',
        at: Date.now(),
        elapsedMs: Date.now() - started,
        limits,
        latestSample: outputJson.monitor.at(-1) ?? null,
        error: describeError(e),
      });
    });
  }, 1000);
  return async () => {
    stopped = true;
    clearInterval(timer);
    while (running) await new Promise((r) => setTimeout(r, 20));
    if (outputJson.monitorUnexpected)
      throw new Error('unexpected monitor failure');
  };
}
function assertHealthy() {
  if (boundError) throw boundError;
}
async function sql(client, text, params = []) {
  assertHealthy();
  const owned = client instanceof Pool ? await client.connect() : client;
  const prior = currentPid;
  currentPid = owned.processID;
  try {
    const result = await owned.query(text, params);
    assertHealthy();
    return result;
  } finally {
    currentPid = prior;
    if (owned !== client) owned.release();
  }
}
async function observedSql(client, text, params = []) {
  currentPid = client.processID;
  try {
    return await sql(client, text, params);
  } finally {
    currentPid = undefined;
  }
}
// Event templates are genuine decoded CBOR maps. Append a valid, deterministic
// extra text field so the record is varied while retaining the original map.
const pad = `CASE WHEN x.template IS NULL THEN NULL ELSE decode(substr(encode(x.template,'hex'),1,4)||lpad(to_hex(get_byte(x.template,2)+1),2,'0')||substr(encode(x.template,'hex'),7)||'6770616464696e67790040'||encode(convert_to(md5(x.id)||md5(x.id||'slice5'),'UTF8'),'hex'),'hex') END`;
// Padding stays inside a valid tagged legacy devalue byte envelope. Each
// segment is keyed by the row identity, avoiding repetitive compressible fill.
function paddedInput(identity, chunks, fallback, isStep = false) {
  if (!chunks) return `decode('${fallback}','hex')`;
  const parts = Array.from(
    { length: chunks },
    (_, i) => `md5((${identity})::text||':slice5:${i}')`
  ).join('||');
  const prefix = isStep
    ? 'devl[{"args":1,"closureVars":-1,"thisVal":-1},[2],"'
    : 'devl[[1],"';
  const text = `'${prefix}'||(${parts})||'"]'`;
  return `decode('d84059'||lpad(to_hex(octet_length(convert_to(${text},'UTF8'))),4,'0'),'hex')||convert_to(${text},'UTF8')`;
}
const FROZEN_PADDING = {
  runChunks: 12,
  stepChunks: 6,
  eventChars: 64,
  algorithm: 'md5-distinct-row-v1',
};
const CALIBRATION = {
  runs: 3000,
  seven: 2196,
  steps: 20196,
  retries: 1200,
  hooks: 2,
  events: 71992,
};
const TARGETS = {
  workflow_runs: 82321408,
  workflow_steps: 386031616,
  workflow_events: 1297825792,
};
async function seed(client, recipe, padding = { runChunks: 0, stepChunks: 0 }) {
  for (let first = 1; first <= recipe.runs; first += 200) {
    const last = Math.min(first + 199, recipe.runs);
    await observedSql(
      client,
      `INSERT INTO workflow.workflow_runs(id,deployment_id,status,name,created_at,updated_at,started_at,completed_at,input_cbor,output_cbor,error_cbor,execution_context_cbor,spec_version)
      SELECT 'wrun_'||lpad(g::text,26,'0'),'postgres',(CASE WHEN g%20=0 THEN 'failed' WHEN g%37=0 THEN 'cancelled' ELSE 'completed' END)::workflow.status,'workflow//slice4a//root',timestamp '2025-01-15 12:00:00'+(g%300)*interval '1 day',timestamp '2025-01-15 12:00:00'+(g%300)*interval '1 day'+(g%2)*interval '1 minute',timestamp '2025-01-15 12:00:01'+(g%300)*interval '1 day',timestamp '2025-01-15 12:01:00'+(g%300)*interval '1 day',${paddedInput('g', padding.runChunks, runInput)},CASE WHEN g%20=0 OR g%37=0 THEN NULL ELSE decode('${runOutput}','hex') END,CASE WHEN g%20=0 THEN $3::bytea ELSE NULL END,decode('${runContext}','hex'),'3' FROM generate_series($1::int,$2::int) g`,
      [
        first,
        last,
        encode({
          message: 'synthetic failure',
          stack: 'Error: synthetic failure',
        }),
      ]
    );
    await observedSql(
      client,
      `INSERT INTO workflow.workflow_steps(run_id,step_id,step_name,status,attempt,created_at,updated_at,started_at,completed_at,input_cbor,output_cbor,spec_version)
      SELECT 'wrun_'||lpad(g::text,26,'0'),'step_'||lpad(((g-1)*7+n)::text,26,'0'),'step//slice4a//business','completed',CASE WHEN (g-1)*7+n<=$4 THEN 2 ELSE 1 END,t,t+interval '30 seconds',t+interval '1 second',t+interval '30 seconds',${paddedInput('(g-1)*7+n', padding.stepChunks, stepInput, true)},decode('${stepOutput}','hex'),3 FROM generate_series($1::int,$2::int) g CROSS JOIN LATERAL generate_series(1,CASE WHEN g<=$3 THEN 7 ELSE 6 END) n CROSS JOIN LATERAL (SELECT timestamp '2025-01-15 12:00:00'+(g%300)*interval '1 day' t) d`,
      [first, last, recipe.seven, recipe.retries]
    );
    const payloads = JSON.stringify(templates);
    await observedSql(
      client,
      `WITH rows AS (
      SELECT g,0 seq,'run_created' type,NULL::text correlation_id FROM generate_series($1::int,$2::int) g
      UNION ALL SELECT g,1,'run_started',NULL FROM generate_series($1::int,$2::int) g
      UNION ALL SELECT g,900,CASE WHEN g%20=0 THEN 'run_failed' WHEN g%37=0 THEN 'run_cancelled' ELSE 'run_completed' END,NULL FROM generate_series($1::int,$2::int) g
      UNION ALL SELECT g,100+n*10+k,CASE k WHEN 0 THEN 'step_created' WHEN 1 THEN 'step_started' ELSE 'step_completed' END,'step_'||lpad(((g-1)*7+n)::text,26,'0') FROM generate_series($1::int,$2::int) g CROSS JOIN LATERAL generate_series(1,CASE WHEN g<=$3 THEN 7 ELSE 6 END) n CROSS JOIN (VALUES(0),(1),(4)) seq(k)
      UNION ALL SELECT g,100+n*10+k,CASE k WHEN 2 THEN 'step_retrying' ELSE 'step_started' END,'step_'||lpad(((g-1)*7+n)::text,26,'0') FROM generate_series($1::int,$2::int) g CROSS JOIN LATERAL generate_series(1,CASE WHEN g<=$3 THEN 7 ELSE 6 END) n CROSS JOIN (VALUES(2),(3)) seq(k) WHERE (g-1)*7+n<=$4
      UNION ALL SELECT g,800+k,CASE k WHEN 0 THEN 'hook_created' ELSE 'hook_received' END,'hook_'||lpad(g::text,26,'0') FROM generate_series($1::int,least($2::int,2)) g CROSS JOIN (VALUES(0),(1)) seq(k)
      ), x AS (SELECT 'evnt_'||lpad((g::bigint*1000+seq)::text,26,'0') id,'wrun_'||lpad(g::text,26,'0') run_id,type,correlation_id,timestamp '2025-01-15 12:00:00'+(g%300)*interval '1 day'+seq*interval '1 millisecond' created_at,CASE WHEN $5::jsonb ? (CASE WHEN type LIKE 'hook_%' THEN type||'_'||g ELSE type END) THEN decode(CASE WHEN type='run_created' THEN replace($5::jsonb->>type,'${runInput}',encode(r.input_cbor,'hex')) WHEN type='step_created' THEN replace($5::jsonb->>type,'${stepInput}',encode(st.input_cbor,'hex')) ELSE $5::jsonb->>(CASE WHEN type LIKE 'hook_%' THEN type||'_'||g ELSE type END) END,'hex') END template FROM rows LEFT JOIN workflow.workflow_runs r ON r.id='wrun_'||lpad(g::text,26,'0') LEFT JOIN workflow.workflow_steps st ON st.step_id=rows.correlation_id)
      INSERT INTO workflow.workflow_events(id,run_id,type,correlation_id,created_at,payload_cbor,spec_version) SELECT x.id,x.run_id,x.type,x.correlation_id,x.created_at,${pad},3 FROM x`,
      [first, last, recipe.seven, recipe.retries, payloads]
    );
  }
  await observedSql(
    client,
    `INSERT INTO workflow.workflow_hooks(run_id,hook_id,token,owner_id,project_id,environment,created_at,spec_version,is_webhook) SELECT 'wrun_'||lpad(g::text,26,'0'),'hook_'||lpad(g::text,26,'0'),'token_'||g,'synthetic','synthetic','test',timestamp '2025-01-15 12:00:00',3,false FROM generate_series(1,2) g`
  );
}
async function tableManifest(pool) {
  return (
    await pool.query(
      `SELECT n.nspname schema,c.relname name,coalesce((SELECT array_agg(a.attname::text ORDER BY k.ord) FROM pg_index i JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord) ON true JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum WHERE i.indrelid=c.oid AND i.indisprimary),'{}') primary_key,coalesce((SELECT array_agg(a.attname::text ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),'{}') columns,coalesce((SELECT array_agg(a.attname::text ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND a.atttypid IN ('timestamp'::regtype,'timestamptz'::regtype)),'{}') timestamps FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' ORDER BY 1,2`
    )
  ).rows;
}
async function digest(db, pool, table, normalize, where = '') {
  const name = q(table.schema) + '.' + q(table.name);
  const count = Number(
    (await sql(pool, 'SELECT count(*)::bigint n FROM ' + name + where)).rows[0]
      .n
  );
  const order = table.primary_key.length
    ? table.primary_key.map((c) => 't.' + q(c)).join(',')
    : 'to_jsonb(t)::text COLLATE "C"';
  const projected = normalize
    ? 'jsonb_build_object(' +
      table.columns
        .map(
          (c) =>
            `'${c.replaceAll("'", "''")}',${table.timestamps.includes(c) ? `extract(epoch from t.${q(c)})` : `to_jsonb(t.${q(c)})`}`
        )
        .join(',') +
      ')'
    : 'to_jsonb(t)';
  const query = `COPY (SELECT (${projected})::text FROM ${name} t ${where} ORDER BY ${order}) TO STDOUT`;
  const { proc, done } = child(
    'docker',
    [
      'exec',
      '-e',
      'PGOPTIONS=-c TimeZone=UTC',
      container,
      'psql',
      '-U',
      'postgres',
      '-d',
      db,
      '-X',
      '-q',
      '-v',
      'ON_ERROR_STOP=1',
      '-c',
      query,
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  const hash = createHash('sha256');
  let err = '';
  proc.stdout.on('data', (c) => hash.update(c));
  proc.stderr.on('data', (c) => (err += c));
  const r = await done;
  if (r.code !== 0) throw new Error('COPY failed ' + err);
  assertHealthy();
  return { count, sha256: hash.digest('hex') };
}
async function snapshot(db, pool) {
  const tables = await tableManifest(pool);
  const raw = {},
    utc = {};
  for (const t of tables) {
    const key = t.schema + '.' + t.name;
    raw[key] = await digest(db, pool, t, false);
    utc[key] = await digest(db, pool, t, true);
  }
  const ddl = (
    await docker('pg_dump', '-U', 'postgres', '--schema-only', db)
  ).stdout.replace(
    /^\\(un)?restrict [A-Za-z0-9]+$/gm,
    (_, un) => `\\${un || ''}restrict <TOKEN>`
  );
  const flags = (
    await sql(
      pool,
      `SELECT n.nspname,c.relname,i.indisvalid,i.indisready,i.indislive,pg_get_indexdef(c.oid) definition FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' ORDER BY 1,2`
    )
  ).rows;
  await writeFile(output + '/' + db + '-schema.sql', ddl);
  const seq = (
    await sql(
      pool,
      `SELECT n.nspname schema,c.relname name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='S' AND n.nspname NOT IN ('pg_catalog','information_schema') ORDER BY 1,2`
    )
  ).rows;
  for (const s of seq)
    Object.assign(
      s,
      (
        await sql(
          pool,
          'SELECT last_value::text,is_called FROM ' +
            q(s.schema) +
            '.' +
            q(s.name)
        )
      ).rows[0]
    );
  return {
    tables,
    raw,
    utc,
    ddlSha256: createHash('sha256').update(ddl).digest('hex'),
    flags,
    seq,
  };
}
async function tableSizes(pool) {
  return (
    await sql(
      pool,
      `SELECT c.relname,pg_relation_size(c.oid)::text heap_bytes,pg_indexes_size(c.oid)::text index_bytes,coalesce(pg_total_relation_size(c.reltoastrelid),0)::text toast_bytes,pg_total_relation_size(c.oid)::text total_bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='workflow' AND c.relname IN ('workflow_runs','workflow_steps','workflow_events') ORDER BY c.relname`
    )
  ).rows;
}
// Phase helpers are intentionally not run in the smoke: exact migration inputs,
// lock samples and the 30-minute migration deadline must be wired before full use.
async function walSnapshot(pool) {
  return (
    await sql(
      pool,
      `SELECT pg_current_wal_insert_lsn()::text lsn, current_setting('wal_level') wal_level,current_setting('full_page_writes') full_page_writes,(SELECT coalesce(sum(size),0)::text FROM pg_ls_waldir()) allocated_wal_bytes`
    )
  ).rows[0];
}
async function lockSnapshot(pool) {
  return {
    activity: (
      await sql(
        pool,
        `SELECT a.pid,a.application_name,a.state,a.wait_event_type,a.wait_event,pg_blocking_pids(a.pid) blockers,l.mode,l.granted,l.relation::regclass::text relation FROM pg_stat_activity a LEFT JOIN pg_locks l ON l.pid=a.pid WHERE a.datname=current_database() ORDER BY a.pid,l.mode`
      )
    ).rows,
    progress: (
      await sql(
        pool,
        `SELECT pid,datname,relid::regclass::text relation,index_relid::regclass::text index_name,phase,lockers_total,lockers_done,blocks_total,blocks_done FROM pg_stat_progress_create_index WHERE datname=current_database()`
      )
    ).rows,
  };
}
async function runMigrationChild(bin, url, extraEnv = {}) {
  migrationStarted = Date.now();
  const { proc, done } = child(process.execPath, [bin], {
    env: {
      ...process.env,
      ...extraEnv,
      PGAPPNAME: 'slice5a-owned-migration-' + process.pid,
      WORKFLOW_POSTGRES_URL: url,
      PGOPTIONS:
        '-c TimeZone=UTC -c workflow.legacy_timezone=UTC -c lock_timeout=5s -c statement_timeout=1800000',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '',
    stderr = '';
  proc.stdout.on('data', (x) => (stdout += x));
  proc.stderr.on('data', (x) => (stderr += x));
  try {
    const exit = await done;
    assertHealthy();
    return { exit, stdout, stderr };
  } finally {
    migrationElapsedMs += Date.now() - migrationStarted;
    migrationStarted = undefined;
  }
}
async function dump(db, path) {
  const { proc, done } = child(
    'docker',
    ['exec', container, 'pg_dump', '-U', 'postgres', '-Fc', db],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  );
  let err = '';
  proc.stderr.on('data', (x) => (err += x));
  await pipeline(proc.stdout, createWriteStream(path));
  const r = await done;
  if (r.code !== 0) throw Error('dump ' + err);
}
async function restore(db, path) {
  const { proc, done } = child(
    'docker',
    [
      'exec',
      '-i',
      container,
      'pg_restore',
      '-U',
      'postgres',
      '-d',
      db,
      '--exit-on-error',
    ],
    { stdio: ['pipe', 'ignore', 'pipe'] }
  );
  let err = '';
  proc.stderr.on('data', (x) => (err += x));
  let pipeError;
  try {
    await pipeline(createReadStream(path), proc.stdin);
  } catch (e) {
    pipeError = e;
  }
  const r = await done;
  if (r.code !== 0 || pipeError)
    throw Error(`restore ${r.code}/${r.signal}: ${err}; pipe: ${pipeError}`);
}
async function fileHashes(root, prefix = '') {
  const result = {};
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const name = prefix + entry.name;
    if (entry.isDirectory())
      Object.assign(
        result,
        await fileHashes(root + '/' + entry.name, name + '/')
      );
    else {
      const bytes = await readFile(root + '/' + entry.name);
      result[name] = createHash('sha256').update(bytes).digest('hex');
    }
  }
  return result;
}
async function volumeRollback(admin) {
  const dumpPath = '/tmp/workflow5-slice5b2-proof/run/full-baseline.dump';
  const dumpStat = await stat(dumpPath);
  assert.equal(dumpStat.size, 403275955);
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(dumpPath)) hash.update(chunk);
  const dumpHash = hash.digest('hex');
  assert.equal(
    dumpHash,
    '05176fa5eb55704c973742eff3c7bb989426fba83cb65c8a6dbf99075da6bed9'
  );
  outputJson.dump = { path: dumpPath, bytes: dumpStat.size, sha256: dumpHash };
  const sample = await usage();
  assert.equal(sample.retainedDump, dumpStat.blocks * 512);
  assert.ok(sample.retainedDump > 0 && sample.guardedOwned < LIMITS.owned);
  outputJson.retainedDumpSample = sample;
  const prior = JSON.parse(
    await readFile('/tmp/workflow5-slice5b2-receipt.json', 'utf8')
  );
  const db = 'slice5c1_rollback_' + process.pid;
  outputJson.database = db;
  await sql(admin, 'CREATE DATABASE ' + q(db));
  await restore(db, dumpPath);
  const url = new URL(adminUrl);
  url.pathname = '/' + db;
  let pool = new Pool({
    connectionString: url.toString(),
    application_name: 'slice5c1-observer',
    options: '-c TimeZone=UTC',
    connectionTimeoutMillis: 10000,
  });
  try {
    outputJson.server = (
      await sql(
        pool,
        `SELECT version() version,current_setting('TimeZone') timezone,current_setting('search_path') search_path,current_setting('fsync') fsync,current_setting('full_page_writes') full_page_writes,current_setting('synchronous_commit') synchronous_commit`
      )
    ).rows[0];
    assert.match(outputJson.server.version, /PostgreSQL 17\.11/);
    assert.equal(outputJson.server.timezone, 'UTC');
    assert.equal(outputJson.server.search_path, '"$user", public');
    assert.equal(outputJson.server.fsync, 'on');
    assert.equal(outputJson.server.full_page_writes, 'on');
    assert.equal(outputJson.server.synchronous_commit, 'on');
    const expectedInspection = { lineage: 'v4.3.10', pendingMigrations: 15 };
    outputJson.preflight = await inspectWorkflowDatabase(pool);
    assert.deepEqual(outputJson.preflight, expectedInspection);
    outputJson.before = await snapshot(db, pool);
    assert.deepEqual(outputJson.before, prior.restored);
    const journal = async () =>
      (
        await sql(
          pool,
          'SELECT id,hash,created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
        )
      ).rows;
    outputJson.journalBefore = await journal();
    assert.deepEqual(outputJson.journalBefore, prior.journal);
    const canonical = fileURLToPath(
      new URL('../drizzle/migrations', import.meta.url)
    );
    const copy = output + '/migrations-copy';
    await cp(canonical, copy, {
      recursive: true,
      errorOnExist: true,
      force: false,
    });
    const sourceHashes = await fileHashes(canonical);
    assert.deepEqual(await fileHashes(copy), sourceHashes);
    const lateFile = '0029_add_run_job_prefix.sql';
    await appendFile(
      copy + '/' + lateFile,
      `\n--> statement-breakpoint\nDO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='workflow' AND table_name='workflow_runs' AND column_name='created_at' AND data_type='timestamp with time zone') OR NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='workflow' AND table_name='workflow_runs' AND column_name='job_prefix') THEN RAISE EXCEPTION 'slice5c1 expected conversion absent'; END IF; END $$;\n--> statement-breakpoint\nSELECT * FROM workflow.slice5c1_deliberately_missing;\n`
    );
    const copiedHashes = await fileHashes(copy);
    assert.deepEqual(
      Object.keys(copiedHashes).sort(),
      Object.keys(sourceHashes).sort()
    );
    for (const name of Object.keys(sourceHashes))
      if (name !== lateFile)
        assert.equal(copiedHashes[name], sourceHashes[name]);
    assert.notEqual(copiedHashes[lateFile], sourceHashes[lateFile]);
    assert.equal(
      sourceHashes['0015a_catch_up_v4_schema.sql'],
      '159c3de7aa454aeb4e59347402546dcb27011f6e0bbaf12c793d406964e049fd'
    );
    outputJson.migrationHashes = {
      canonical: sourceHashes,
      copied: copiedHashes,
      soleChangedFile: lateFile,
    };
    outputJson.walBefore = await walSnapshot(pool);
    await pool.end();
    pool = undefined;
    const start = Date.now();
    const childResult = await runMigrationChild(
      fileURLToPath(
        new URL('./migration-volume-rollback-worker.mjs', import.meta.url)
      ),
      url.toString(),
      { SLICE5_MIGRATIONS_FOLDER: copy }
    );
    outputJson.migrationElapsedMs = Date.now() - start;
    await writeFile(
      output + '/rollback-worker.log',
      childResult.stdout + childResult.stderr
    );
    outputJson.worker = {
      exit: childResult.exit,
      lines: childResult.stdout
        .trim()
        .split('\n')
        .map((s) => JSON.parse(s)),
      stderr: childResult.stderr,
    };
    assert.deepEqual(childResult.exit, { code: 1, signal: null });
    const settings = outputJson.worker.lines[0].settings;
    assert.deepEqual(
      {
        timezone: settings.timezone,
        legacy_timezone: settings.legacy_timezone,
        lock_timeout: settings.lock_timeout,
        statement_timeout: settings.statement_timeout,
      },
      {
        timezone: 'UTC',
        legacy_timezone: 'UTC',
        lock_timeout: '5s',
        statement_timeout: '30min',
      }
    );
    assert.equal(
      settings.application_name,
      'slice5a-owned-migration-' + process.pid
    );
    assert.ok(
      outputJson.worker.lines[1].causes.some(
        (cause) =>
          cause.code === '42P01' &&
          cause.message.includes('slice5c1_deliberately_missing')
      )
    );
    pool = new Pool({
      connectionString: url.toString(),
      application_name: 'slice5c1-after',
      options: '-c TimeZone=UTC',
      connectionTimeoutMillis: 10000,
    });
    outputJson.walAfter = await walSnapshot(pool);
    outputJson.inspectionAfter = await inspectWorkflowDatabase(pool);
    assert.deepEqual(outputJson.inspectionAfter, expectedInspection);
    outputJson.after = await snapshot(db, pool);
    for (const key of ['tables', 'raw', 'utc', 'ddlSha256', 'flags'])
      assert.deepEqual(outputJson.after[key], outputJson.before[key]);
    const expectedSeq = structuredClone(outputJson.before.seq);
    const sequence = expectedSeq.find(
      (s) =>
        s.schema === 'workflow_drizzle' &&
        s.name === 'workflow_migrations_id_seq'
    );
    assert.deepEqual(
      { last_value: sequence.last_value, is_called: sequence.is_called },
      { last_value: '12', is_called: true }
    );
    sequence.last_value = '26';
    assert.deepEqual(outputJson.after.seq, expectedSeq);
    outputJson.journalAfter = await journal();
    assert.deepEqual(outputJson.journalAfter, outputJson.journalBefore);
    outputJson.remainingWorkers = (
      await sql(
        pool,
        `SELECT pid FROM pg_stat_activity WHERE application_name=$1`,
        ['slice5a-owned-migration-' + process.pid]
      )
    ).rows;
    assert.deepEqual(outputJson.remainingWorkers, []);
    outputJson.comparisons = {
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
    };
    assert.deepEqual(await fileHashes(canonical), sourceHashes);
  } finally {
    await pool?.end();
  }
}
async function volumeSuccess(admin) {
  const dumpPath = '/tmp/workflow5-slice5b2-proof/run/full-baseline.dump';
  const dumpStat = await stat(dumpPath);
  assert.equal(dumpStat.size, 403275955);
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(dumpPath)) hash.update(chunk);
  const dumpHash = hash.digest('hex');
  assert.equal(
    dumpHash,
    '05176fa5eb55704c973742eff3c7bb989426fba83cb65c8a6dbf99075da6bed9'
  );
  outputJson.dump = { path: dumpPath, bytes: dumpStat.size, sha256: dumpHash };
  outputJson.retainedDumpSample = await usage();
  assert.equal(
    outputJson.retainedDumpSample.retainedDump,
    dumpStat.blocks * 512
  );
  const prior = JSON.parse(
    await readFile('/tmp/workflow5-slice5b2-receipt.json', 'utf8')
  );
  const db = 'slice5c2_success_' + process.pid;
  outputJson.database = db;
  await sql(admin, 'CREATE DATABASE ' + q(db));
  await restore(db, dumpPath);
  const url = new URL(adminUrl);
  url.pathname = '/' + db;
  const pool = new Pool({
    connectionString: url.toString(),
    application_name: 'slice5c2-observer',
    options: '-c TimeZone=UTC',
    connectionTimeoutMillis: 10000,
  });
  const readPool = new Pool({
    connectionString: url.toString(),
    application_name: 'slice5c2-probe',
    options: '-c TimeZone=UTC -c statement_timeout=500',
    connectionTimeoutMillis: 10000,
    max: 1,
  });
  let holder, sampler, probeTask, bootstrap;
  let finished = false,
    samplingFailure;
  try {
    outputJson.server = (
      await sql(
        pool,
        `SELECT version() version,current_setting('TimeZone') timezone,current_setting('search_path') search_path,current_setting('fsync') fsync,current_setting('full_page_writes') full_page_writes,current_setting('synchronous_commit') synchronous_commit,current_setting('checkpoint_timeout') checkpoint_timeout,current_setting('max_wal_size') max_wal_size,current_setting('wal_compression') wal_compression`
      )
    ).rows[0];
    assert.match(outputJson.server.version, /PostgreSQL 17\.11/);
    assert.equal(outputJson.server.timezone, 'UTC');
    assert.equal(outputJson.server.search_path, '"$user", public');
    assert.equal(outputJson.server.fsync, 'on');
    assert.equal(outputJson.server.full_page_writes, 'on');
    assert.equal(outputJson.server.synchronous_commit, 'on');
    outputJson.preflight = await inspectWorkflowDatabase(pool);
    assert.deepEqual(outputJson.preflight, {
      lineage: 'v4.3.10',
      pendingMigrations: 15,
    });
    outputJson.before = await snapshot(db, pool);
    assert.deepEqual(outputJson.before, prior.restored);
    const journal = async () =>
      (
        await sql(
          pool,
          'SELECT id,hash,created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
        )
      ).rows;
    outputJson.journalBefore = await journal();
    assert.deepEqual(outputJson.journalBefore, prior.journal);
    const canonical = fileURLToPath(
      new URL('../drizzle/migrations', import.meta.url)
    );
    outputJson.migrationHashes = await fileHashes(canonical);
    assert.equal(
      outputJson.migrationHashes['0015a_catch_up_v4_schema.sql'],
      '159c3de7aa454aeb4e59347402546dcb27011f6e0bbaf12c793d406964e049fd'
    );
    outputJson.sizesBefore = await tableSizes(pool);
    outputJson.walBefore = await walSnapshot(pool);
    const bridge = await import(
      pathToFileURL(pulsify + '/apps/api/scripts/workflow-major-cutover.ts')
        .href
    );
    const candidate = fileURLToPath(
      new URL('../../bin/setup.js', import.meta.url)
    );
    const audit = fileURLToPath(
      new URL('./migration-volume-session-audit.mjs', import.meta.url)
    );
    const auditPath = output + '/session-audit.jsonl';
    const priorEnv = {
      PGAPPNAME: env.PGAPPNAME,
      PGOPTIONS: env.PGOPTIONS,
      NODE_OPTIONS: env.NODE_OPTIONS,
      SLICE5_SESSION_AUDIT: env.SLICE5_SESSION_AUDIT,
    };
    const restoreEnv = () => {
      for (const [key, value] of Object.entries(priorEnv))
        if (value === undefined) delete env[key];
        else env[key] = value;
    };
    const official = async (label) => {
      env.PGAPPNAME = 'slice5a-owned-migration-' + process.pid;
      env.PGOPTIONS = '-c TimeZone=UTC -c workflow.legacy_timezone=UTC';
      env.NODE_OPTIONS = '--import=' + pathToFileURL(audit).href;
      env.SLICE5_SESSION_AUDIT = auditPath;
      migrationAbort = new AbortController();
      migrationStarted = Date.now();
      const at = migrationStarted;
      try {
        await bridge.withBootstrapLock(
          url.toString(),
          migrationAbort.signal,
          async (pid, signal) => {
            outputJson[label + 'OwnerPid'] = pid;
            await bridge.runBootstrapProcess(candidate, url.toString(), signal);
          }
        );
        assertHealthy();
        return { exit: 0, elapsedMs: Date.now() - at };
      } finally {
        migrationElapsedMs += Date.now() - migrationStarted;
        migrationStarted = undefined;
        migrationAbort = undefined;
        restoreEnv();
      }
    };
    holder = await pool.connect();
    await holder.query('BEGIN');
    await holder.query('SELECT id FROM workflow.workflow_events LIMIT 1');
    const holderPid = holder.processID;
    outputJson.holderPid = holderPid;
    outputJson.lockSamples = [];
    outputJson.readProbes = [];
    let observed;
    let resolveBlocked;
    const blocked = new Promise((resolve) => {
      resolveBlocked = resolve;
    });
    bootstrap = official('migration').then(
      (value) => {
        finished = true;
        return value;
      },
      (error) => {
        finished = true;
        throw error;
      }
    );
    // Attach a handler immediately: observer and child can fail independently.
    bootstrap.catch(() => {});
    sampler = (async () => {
      while (!finished) {
        try {
          const rows = (
            await pool.query(
              `SELECT clock_timestamp() sampled_at,a.pid,a.application_name,a.state,a.wait_event_type,a.wait_event,a.xact_start,pg_blocking_pids(a.pid) blockers,left(a.query,250) query,l.mode,l.granted,l.relation::regclass::text relation FROM pg_stat_activity a LEFT JOIN pg_locks l ON l.pid=a.pid WHERE a.datname=current_database() AND a.application_name=$1 AND a.pid<>coalesce($2,0) ORDER BY a.pid,l.mode`,
              [
                'slice5a-owned-migration-' + process.pid,
                outputJson.migrationOwnerPid ?? 0,
              ]
            )
          ).rows;
          const progress = (
            await pool.query(
              `SELECT pid,relid::regclass::text relation,index_relid::regclass::text index_name,phase,lockers_total,lockers_done,blocks_total,blocks_done FROM pg_stat_progress_create_index WHERE datname=current_database()`
            )
          ).rows;
          const sample = { at: Date.now(), rows, progress };
          outputJson.lockSamples.push(sample);
          const waiting = rows.find(
            (row) => row.blockers.includes(holderPid) && !row.granted
          );
          if (waiting && !observed) {
            observed = { at: sample.at, pid: waiting.pid, row: waiting };
            resolveBlocked(observed);
          }
        } catch (error) {
          samplingFailure = error;
          migrationAbort?.abort(error);
          resolveBlocked(null);
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
    })();
    const waitResult = await Promise.race([
      blocked,
      bootstrap.then(
        () => null,
        () => null
      ),
      new Promise((resolve) => setTimeout(() => resolve(null), 9000)),
    ]);
    assert.ok(waitResult, 'No actual blocking lock observed');
    const readStart = Date.now();
    try {
      await readPool.query('SELECT id FROM workflow.workflow_events LIMIT 1');
      throw new Error('Expected blocked reader timeout');
    } catch (error) {
      assert.equal(error.code, '57014');
      outputJson.blockedReader = {
        code: error.code,
        elapsedMs: Date.now() - readStart,
      };
    }
    await holder.query('ROLLBACK');
    holder.release();
    holder = undefined;
    outputJson.blocked = {
      ...observed,
      holderPid,
      releasedAt: Date.now(),
      dwellMs: Date.now() - observed.at,
    };
    assert.ok(outputJson.blocked.dwellMs < 3000);
    probeTask = (async () => {
      while (!finished) {
        const startedAt = Date.now();
        try {
          await readPool.query(
            'SELECT id FROM workflow.workflow_events LIMIT 1'
          );
          outputJson.readProbes.push({
            at: startedAt,
            elapsedMs: Date.now() - startedAt,
            result: 'ok',
          });
        } catch (error) {
          outputJson.readProbes.push({
            at: startedAt,
            elapsedMs: Date.now() - startedAt,
            result: error.code,
          });
          if (error.code !== '57014') {
            migrationAbort?.abort(error);
            throw error;
          }
        }
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
    })();
    outputJson.migration = await bootstrap;
    await sampler;
    await probeTask;
    if (samplingFailure) throw samplingFailure;
    const auditRows = (await readFile(auditPath, 'utf8'))
      .trim()
      .split('\n')
      .map((s) => JSON.parse(s));
    const migrationSession = auditRows.find((row) => row.pid === observed.pid);
    assert.ok(
      migrationSession,
      'Observed migrator PID absent from session audit'
    );
    assert.deepEqual(
      {
        timezone: migrationSession.timezone,
        legacy_timezone: migrationSession.legacy_timezone,
        lock_timeout: migrationSession.lock_timeout,
        statement_timeout: migrationSession.statement_timeout,
      },
      {
        timezone: 'UTC',
        legacy_timezone: 'UTC',
        lock_timeout: '5s',
        statement_timeout: '30min',
      }
    );
    outputJson.sessionAudit = auditRows;
    outputJson.migratorPid = observed.pid;
    assert.ok(
      outputJson.lockSamples.some((sample) =>
        sample.rows.some(
          (row) =>
            row.pid === observed.pid &&
            row.granted &&
            row.mode === 'AccessExclusiveLock' &&
            row.relation?.endsWith('workflow_events')
        )
      )
    );
    await readPool.query('SELECT id FROM workflow.workflow_events LIMIT 1');
    outputJson.postCommitRead = 'ok';
    outputJson.walAfter = await walSnapshot(pool);
    outputJson.sizesAfter = await tableSizes(pool);
    outputJson.inspectionAfter = await inspectWorkflowDatabase(pool);
    assert.deepEqual(outputJson.inspectionAfter, {
      lineage: 'v5.1.0',
      pendingMigrations: 0,
    });
    outputJson.journalAfter = await journal();
    assert.equal(outputJson.journalAfter.length, 27);
    assert.deepEqual(
      outputJson.journalAfter.slice(0, 12),
      outputJson.journalBefore
    );
    const compareOld = async () => {
      const result = {};
      for (const table of outputJson.before.tables) {
        const key = table.schema + '.' + table.name;
        const where =
          key === 'workflow_drizzle.workflow_migrations'
            ? ' WHERE id <= 12'
            : '';
        result[key] = await digest(db, pool, table, true, where);
        assert.deepEqual(result[key], outputJson.before.utc[key]);
      }
      return result;
    };
    outputJson.postMigrationDigests = await compareOld();
    const targetChecks = async () => {
      const checks = (
        await sql(
          pool,
          `SELECT (SELECT count(*) FROM workflow.workflow_runs WHERE attributes <> '{}'::jsonb OR error_code IS NOT NULL OR encryption_public_key IS NOT NULL OR dynamic_workflow_code_cbor IS NOT NULL OR start_hook_cbor IS NOT NULL OR job_prefix IS NOT NULL)::int bad_runs,(SELECT count(*) FROM workflow.workflow_hooks WHERE is_system IS DISTINCT FROM false OR resume_context IS NOT NULL OR token_retention_until IS NOT NULL OR claimed_from IS NOT NULL OR claims_token IS DISTINCT FROM false)::int bad_hooks,(SELECT count(*) FROM workflow.workflow_events WHERE resume_id IS NOT NULL OR resume_payload_digest IS NOT NULL)::int bad_events,(SELECT count(*) FROM workflow.workflow_event_slots)::int slots,(SELECT count(*) FROM workflow.workflow_invocations)::int invocations,(SELECT count(*) FROM workflow.workflow_snapshots)::int snapshots,(SELECT count(*) FROM workflow.workflow_start_hook_admissions)::int admissions`
        )
      ).rows[0];
      assert.deepEqual(checks, {
        bad_runs: 0,
        bad_hooks: 0,
        bad_events: 0,
        slots: 0,
        invocations: 0,
        snapshots: 0,
        admissions: 0,
      });
      const indexes = (
        await sql(
          pool,
          `SELECT n.nspname,c.relname,i.indisvalid,i.indisready,i.indislive FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' ORDER BY 1,2`
        )
      ).rows;
      assert.ok(
        indexes.every(
          (index) => index.indisvalid && index.indisready && index.indislive
        )
      );
      return { checks, indexes };
    };
    outputJson.targetChecks = await targetChecks();
    outputJson.noop = await official('noop');
    assert.deepEqual(
      await inspectWorkflowDatabase(pool),
      outputJson.inspectionAfter
    );
    assert.deepEqual(await journal(), outputJson.journalAfter);
    outputJson.noopDigests = await compareOld();
    assert.deepEqual(await targetChecks(), outputJson.targetChecks);
    assert.deepEqual(await fileHashes(canonical), outputJson.migrationHashes);
    outputJson.comparisons = {
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
    };
  } finally {
    finished = true;
    if (holder) {
      await holder.query('ROLLBACK').catch(() => {});
      holder.release();
    }
    if (bootstrap) {
      if (migrationAbort) migrationAbort.abort(new Error('fixture cleanup'));
      await bootstrap.catch(() => {});
    }
    if (sampler) await sampler.catch(() => {});
    if (probeTask) await probeTask.catch(() => {});
    await readPool.end();
    await pool.end();
  }
}
if (fixtureMode === 'low-control' || fixtureMode === 'sample-error-control') {
  let lowPool, lowStop, sleeper;
  try {
    lowPool = new Pool({
      connectionString: adminUrl,
      application_name: 'slice5a-low-control',
      connectionTimeoutMillis: 10000,
    });
    controller = lowPool;
    sleeper = await lowPool.connect();
    currentPid = sleeper.processID;
    const sleeping = sleeper.query('SELECT pg_sleep(60)').then(
      () => ({ code: 'unexpected success' }),
      (e) => ({ code: e.code, message: e.message })
    );
    for (let i = 0; i < 100; i++) {
      const state = await lowPool.query(
        'SELECT state FROM pg_stat_activity WHERE pid=$1',
        [currentPid]
      );
      if (state.rows[0]?.state === 'active') break;
      await new Promise((r) => setTimeout(r, 10));
    }
    const sqlPid = currentPid;
    const probe = child(process.execPath, ['-e', 'setInterval(()=>{},1000)'], {
      stdio: 'ignore',
    });
    const measurement = await usage();
    assert.ok(measurement.pgdata + measurement.receipt > 1);
    lowStop =
      fixtureMode === 'low-control'
        ? await startMonitor({ ...LIMITS, owned: 1 })
        : await startMonitor(LIMITS, true);
    await lowStop();
    lowStop = undefined;
    const exit = await probe.done,
      sqlExit = await sleeping;
    currentPid = undefined;
    assert.equal(sqlExit.code, '57014');
    assert.equal(exit.signal, 'SIGTERM');
    assert.equal(children.has(probe.proc), false);
    assert.ok(boundError);
    const savedFirst = JSON.stringify(firstCancellation);
    await cancelOwned({ category: 'secondary_error', at: Date.now() });
    assert.equal(JSON.stringify(firstCancellation), savedFirst);
    await writeFile(
      output + '/' + fixtureMode + '.json',
      JSON.stringify(
        {
          threshold: fixtureMode === 'low-control' ? 1 : LIMITS.owned,
          measurement,
          childPid: probe.proc.pid,
          sqlPid,
          sqlExit,
          exit,
          reaped: true,
          controlledError: boundError.message,
          controllerReason: firstCancellation,
          reasonFile: outputJson.cancelReasonPath,
          cancellationErrors: outputJson.cancellationErrors ?? [],
        },
        null,
        2
      )
    );
  } finally {
    if (lowStop) await lowStop();
    for (const p of children) if (!p.killed) p.kill('SIGTERM');
    while (children.size) await new Promise((r) => setTimeout(r, 20));
    sleeper?.release();
    await lowPool?.end();
  }
  process.exitCode = 1;
} else {
  let stopMonitor, admin, client, restored;
  try {
    admin = new Pool({
      connectionString: adminUrl,
      application_name: 'slice5a-control',
      connectionTimeoutMillis: 10000,
    });
    controller = admin;
    stopMonitor = await startMonitor();
    assertHealthy();
    if (fixtureMode === 'volume-rollback') {
      await volumeRollback(admin);
    } else if (fixtureMode === 'volume-success') {
      await volumeSuccess(admin);
    } else {
      const recipe = fixtureMode === 'seed-volume' ? FULL : SMOKE;
      if (fixtureMode === 'seed-volume') {
        const calDb = 'slice5b_calibration_' + process.pid;
        await sql(admin, 'CREATE DATABASE ' + q(calDb));
        const calUrl = new URL(adminUrl);
        calUrl.pathname = '/' + calDb;
        const oldBin =
          pulsify +
          '/apps/api/node_modules/@workflow/world-postgres/bin/setup.js';
        await run(process.execPath, [oldBin], {
          env: {
            ...env,
            WORKFLOW_POSTGRES_URL: calUrl.toString(),
            TZ: 'UTC',
            PGAPPNAME: 'slice5b-calibration-bootstrap',
          },
        });
        const calPool = new Pool({
          connectionString: calUrl.toString(),
          application_name: 'slice5b-calibration',
          options: '-c TimeZone=UTC',
          connectionTimeoutMillis: 10000,
        });
        try {
          assert.deepEqual(await inspectWorkflowDatabase(calPool), {
            lineage: 'v4.3.10',
            pendingMigrations: 15,
          });
          const c = await calPool.connect();
          try {
            await seed(c, CALIBRATION, FROZEN_PADDING);
          } finally {
            c.release();
          }
          const sample = (
            await sql(
              calPool,
              `SELECT r.input_cbor run_input,er.payload_cbor run_event,s.input_cbor step_input,es.payload_cbor step_event FROM workflow.workflow_runs r JOIN workflow.workflow_events er ON er.run_id=r.id AND er.type='run_created' JOIN workflow.workflow_steps s ON s.run_id=r.id JOIN workflow.workflow_events es ON es.correlation_id=s.step_id AND es.type='step_created' ORDER BY r.id,s.step_id LIMIT 1`
            )
          ).rows[0];
          assert.deepEqual(
            decode(sample.run_input),
            decode(sample.run_event).input
          );
          assert.deepEqual(
            decode(sample.step_input),
            decode(sample.step_event).input
          );
          const requireCore = createRequire(
            new URL('../../../core/package.json', import.meta.url)
          );
          const { parse } = await import(
            pathToFileURL(requireCore.resolve('devalue'))
          );
          const runText = Buffer.from(decode(sample.run_input)).toString();
          const stepText = Buffer.from(decode(sample.step_input)).toString();
          assert.equal(
            parse(runText.slice(4))[0].length,
            FROZEN_PADDING.runChunks * 32
          );
          assert.equal(
            parse(stepText.slice(4)).args[0].length,
            FROZEN_PADDING.stepChunks * 32
          );
          const calCounts = (
            await sql(
              calPool,
              `SELECT (SELECT count(*)::int FROM workflow.workflow_runs) runs,(SELECT count(*)::int FROM workflow.workflow_steps) steps,(SELECT count(*)::int FROM workflow.workflow_events) events`
            )
          ).rows[0];
          assert.deepEqual(calCounts, {
            runs: CALIBRATION.runs,
            steps: CALIBRATION.steps,
            events: CALIBRATION.events,
          });
          await sql(calPool, 'ANALYZE');
          const rows = await tableSizes(calPool);
          const projections = {};
          for (const row of rows) {
            const rate =
              Number(row.total_bytes) /
              {
                workflow_runs: CALIBRATION.runs,
                workflow_steps: CALIBRATION.steps,
                workflow_events: CALIBRATION.events,
              }[row.relname];
            const projected =
              rate *
              {
                workflow_runs: FULL.runs,
                workflow_steps: FULL.steps,
                workflow_events: FULL.events,
              }[row.relname];
            projections[row.relname] = {
              sampleBytes: Number(row.total_bytes),
              bytesPerRow: rate,
              projectedBytes: projected,
              targetBytes: TARGETS[row.relname],
              ratio: projected / TARGETS[row.relname],
            };
          }
          outputJson.calibration = {
            attempt: 1,
            recipe: CALIBRATION,
            padding: FROZEN_PADDING,
            counts: calCounts,
            decodedInputValidated: true,
            sizes: rows,
            projections,
          };
          await writeFile(
            output + '/calibration.json',
            JSON.stringify(outputJson.calibration, null, 2)
          );
          assert.ok(
            Object.values(projections).every(
              (p) => p.ratio >= 0.8 && p.ratio <= 1.2
            ),
            'calibration projection outside tolerance; full seed not started'
          );
        } finally {
          await calPool.end();
        }
        await sql(admin, 'DROP DATABASE ' + q(calDb));
        const self = await import('node:fs/promises').then((fs) =>
          fs.readFile(fileURLToPath(import.meta.url))
        );
        outputJson.freeze = {
          sourceSha256: createHash('sha256').update(self).digest('hex'),
          recipe: FULL,
          padding: FROZEN_PADDING,
          calibrationAttempts: 1,
          seed: 'slice5 deterministic md5-distinct-row-v1',
        };
        await writeFile(
          output + '/frozen-recipe.json',
          JSON.stringify(outputJson.freeze, null, 2)
        );
      }
      const db = 'slice5b_' + fixtureMode.replace('-', '_') + '_' + process.pid,
        db2 = db + '_restore';
      await sql(admin, 'CREATE DATABASE ' + q(db));
      const url = new URL(adminUrl);
      url.pathname = '/' + db;
      const childEnv = {
        ...env,
        WORKFLOW_POSTGRES_URL: url.toString(),
        TZ: 'UTC',
        PGAPPNAME: 'slice5a-old-bootstrap',
      };
      await run(
        process.execPath,
        [
          pulsify +
            '/apps/api/node_modules/@workflow/world-postgres/bin/setup.js',
        ],
        { env: childEnv }
      );
      client = new Pool({
        connectionString: url.toString(),
        application_name: 'slice5a-seed',
        options: '-c TimeZone=UTC',
        connectionTimeoutMillis: 10000,
      });
      const oldEntry = await import('node:fs/promises').then((fs) =>
        fs.realpath(
          pulsify +
            '/apps/api/node_modules/@workflow/world-postgres/bin/setup.js'
        )
      );
      const requireHere = createRequire(import.meta.url);
      const identities = {
        oldSetup: oldEntry,
        oldSetupSha256: createHash('sha256')
          .update(
            await import('node:fs/promises').then((fs) => fs.readFile(oldEntry))
          )
          .digest('hex'),
        targetCli: await import('node:fs/promises').then((fs) =>
          fs.realpath(new URL('../../dist/cli.js', import.meta.url))
        ),
        pg: requireHere.resolve('pg'),
        cbor: requireHere.resolve('cbor-x'),
        node: process.version,
      };
      for (const k of ['targetCli', 'pg', 'cbor'])
        identities[k + 'Sha256'] = createHash('sha256')
          .update(
            await import('node:fs/promises').then((fs) =>
              fs.readFile(identities[k])
            )
          )
          .digest('hex');
      outputJson.identities = identities;
      outputJson.host = {
        cpuCount: cpus().length,
        cpuModel: cpus()[0]?.model,
        totalMemory: totalmem(),
        freeMemory: freemem(),
      };
      const info = (
        await sql(
          client,
          `SELECT version(),current_setting('TimeZone') timezone,current_setting('search_path') search_path,current_setting('fsync') fsync,current_setting('full_page_writes') full_page_writes,current_setting('synchronous_commit') synchronous_commit,current_setting('wal_level') wal_level,current_setting('server_encoding') encoding,current_setting('application_name') application_name,pg_backend_pid() pid`
        )
      ).rows[0];
      outputJson.server = info;
      if (fixtureMode === 'monitor') {
        const churn = child(
          'docker',
          [
            'exec',
            container,
            'bash',
            '-c',
            'set -e; d=/var/lib/postgresql/data/slice5b-owned-churn; mkdir "$d"; for i in $(seq 1 250); do mkdir "$d/$i"; for j in $(seq 1 40); do : > "$d/$i/$j"; done; rm -r "$d/$i"; sleep 0.01; done; rmdir "$d"',
          ],
          { stdio: ['ignore', 'ignore', 'pipe'] }
        );
        let stderr = '';
        churn.proc.stderr.on('data', (c) => (stderr += c));
        const observations = [];
        for (let i = 0; i < 12; i++) {
          assert.equal(
            churn.proc.exitCode,
            null,
            'churn stopped before sample'
          );
          assert.equal(churn.proc.signalCode, null, 'churn was signaled');
          observations.push(await usage());
          await new Promise((r) => setTimeout(r, 200));
        }
        const exit = await churn.done;
        assert.deepEqual(exit, { code: 0, signal: null });
        assertHealthy();
        outputJson.churn = {
          exit,
          stderr,
          samples: observations.length,
          observations,
        };
      }
      outputJson.preflight = await inspectWorkflowDatabase(client);
      assert.deepEqual(outputJson.preflight, {
        lineage: 'v4.3.10',
        pendingMigrations: 15,
      });
      outputJson.journal = (
        await sql(
          client,
          'SELECT id,hash,created_at FROM workflow_drizzle.workflow_migrations ORDER BY id'
        )
      ).rows;
      assert.equal(outputJson.journal.length, 12);
      const seedClient = await client.connect();
      try {
        await seed(
          seedClient,
          recipe,
          fixtureMode === 'seed-volume' ? FROZEN_PADDING : undefined
        );
      } finally {
        seedClient.release();
      }
      outputJson.postSeedInspection = await inspectWorkflowDatabase(client);
      assert.deepEqual(outputJson.postSeedInspection, outputJson.preflight);
      const sample = (
        await sql(
          client,
          "SELECT type,payload_cbor FROM workflow.workflow_events WHERE type='run_created' ORDER BY id LIMIT 1"
        )
      ).rows[0];
      const decoded = decode(sample.payload_cbor);
      assert.equal(decoded.workflowName, 'workflow//slice4a//root');
      assert.equal(decoded.padding.length, 64);
      const coherentRun = (
        await sql(
          client,
          `SELECT r.input_cbor,r.output_cbor,e.payload_cbor,ec.payload_cbor created FROM workflow.workflow_runs r JOIN workflow.workflow_events e ON e.run_id=r.id AND e.type='run_completed' JOIN workflow.workflow_events ec ON ec.run_id=r.id AND ec.type='run_created' WHERE r.status='completed' ORDER BY r.id LIMIT 1`
        )
      ).rows[0];
      assert.deepEqual(
        decode(coherentRun.input_cbor),
        decode(coherentRun.created).input
      );
      assert.deepEqual(
        decode(coherentRun.output_cbor),
        decode(coherentRun.payload_cbor).output
      );
      const coherentStep = (
        await sql(
          client,
          `SELECT s.input_cbor,s.output_cbor,ec.payload_cbor created,ed.payload_cbor completed FROM workflow.workflow_steps s JOIN workflow.workflow_events ec ON ec.run_id=s.run_id AND ec.correlation_id=s.step_id AND ec.type='step_created' JOIN workflow.workflow_events ed ON ed.run_id=s.run_id AND ed.correlation_id=s.step_id AND ed.type='step_completed' ORDER BY s.step_id LIMIT 1`
        )
      ).rows[0];
      assert.deepEqual(
        decode(coherentStep.input_cbor),
        decode(coherentStep.created).input
      );
      assert.deepEqual(
        decode(coherentStep.output_cbor),
        decode(coherentStep.completed).result
      );
      const coherentHooks = (
        await sql(
          client,
          `SELECT h.token,e.payload_cbor FROM workflow.workflow_hooks h JOIN workflow.workflow_events e ON e.run_id=h.run_id AND e.correlation_id=h.hook_id AND e.type='hook_created' ORDER BY h.hook_id`
        )
      ).rows;
      assert.equal(coherentHooks.length, 2);
      for (const h of coherentHooks)
        assert.equal(decode(h.payload_cbor).token, h.token);
      if (fixtureMode === 'seed-volume') {
        const requireCore = createRequire(
          new URL('../../../core/package.json', import.meta.url)
        );
        const { parse } = await import(
          pathToFileURL(requireCore.resolve('devalue'))
        );
        const runText = Buffer.from(decode(coherentRun.input_cbor)).toString();
        const stepText = Buffer.from(
          decode(coherentStep.input_cbor)
        ).toString();
        assert.ok(runText.startsWith('devl') && stepText.startsWith('devl'));
        const r = parse(runText.slice(4)),
          st = parse(stepText.slice(4));
        assert.equal(r[0].length, FROZEN_PADDING.runChunks * 32);
        assert.equal(st.args[0].length, FROZEN_PADDING.stepChunks * 32);
        outputJson.decodedSyntheticInput = {
          runPadding: r[0].length,
          stepPadding: st.args[0].length,
        };
      }
      outputJson.payloadCoherence = { run: true, step: true, hooks: true };
      outputJson.envelopeCheck = {
        eventType: sample.type,
        workflowName: decoded.workflowName,
        paddingLength: decoded.padding.length,
      };
      const counts = (
        await sql(
          client,
          `SELECT (SELECT count(*)::int FROM workflow.workflow_runs) runs,(SELECT count(*)::int FROM workflow.workflow_steps) steps,(SELECT count(*)::int FROM workflow.workflow_events) events,(SELECT count(*)::int FROM workflow.workflow_hooks) hooks,(SELECT count(*)::int FROM graphile_worker._private_jobs) jobs,(SELECT count(*)::int FROM workflow.workflow_runs WHERE status NOT IN ('completed','failed','cancelled')) active,(SELECT count(*)::int FROM workflow.workflow_events e LEFT JOIN workflow.workflow_runs r ON r.id=e.run_id WHERE r.id IS NULL) orphan_events,(SELECT count(*)::int FROM workflow.workflow_steps s LEFT JOIN workflow.workflow_runs r ON r.id=s.run_id WHERE r.id IS NULL) orphan_steps`
        )
      ).rows[0];
      assert.deepEqual(counts, {
        runs: recipe.runs,
        steps: recipe.steps,
        events: recipe.events,
        hooks: 2,
        jobs: 0,
        active: 0,
        orphan_events: 0,
        orphan_steps: 0,
      });
      outputJson.counts = counts;
      outputJson.integrity = (
        await sql(
          client,
          `SELECT (SELECT count(*)::int FROM workflow.workflow_events e LEFT JOIN workflow.workflow_steps s ON s.step_id=e.correlation_id AND s.run_id=e.run_id WHERE e.type LIKE 'step_%' AND s.step_id IS NULL) missing_step,(SELECT count(*)::int FROM workflow.workflow_events e LEFT JOIN workflow.workflow_hooks h ON h.hook_id=e.correlation_id AND h.run_id=e.run_id WHERE e.type LIKE 'hook_%' AND h.hook_id IS NULL) missing_hook,(SELECT count(*)::int FROM workflow.workflow_steps s WHERE (SELECT count(*) FROM workflow.workflow_events e WHERE e.run_id=s.run_id AND e.correlation_id=s.step_id AND e.type='step_created')<>1 OR (SELECT count(*) FROM workflow.workflow_events e WHERE e.run_id=s.run_id AND e.correlation_id=s.step_id AND e.type='step_completed')<>1) invalid_step_lifecycle,(SELECT count(*)::int FROM workflow.workflow_runs r WHERE (SELECT count(*) FROM workflow.workflow_events e WHERE e.run_id=r.id AND e.type='run_created')<>1 OR (SELECT count(*) FROM workflow.workflow_events e WHERE e.run_id=r.id AND e.type IN ('run_completed','run_failed','run_cancelled'))<>1) invalid_run_lifecycle`
        )
      ).rows[0];
      assert.deepEqual(outputJson.integrity, {
        missing_step: 0,
        missing_hook: 0,
        invalid_step_lifecycle: 0,
        invalid_run_lifecycle: 0,
      });
      outputJson.invalidIds = Number(
        (
          await sql(
            client,
            `SELECT (SELECT count(*) FROM workflow.workflow_runs WHERE id !~ '^wrun_[0-7][0-9A-HJKMNP-TV-Z]{25}$') + (SELECT count(*) FROM workflow.workflow_steps WHERE step_id !~ '^step_[0-7][0-9A-HJKMNP-TV-Z]{25}$') + (SELECT count(*) FROM workflow.workflow_events WHERE id !~ '^evnt_[0-7][0-9A-HJKMNP-TV-Z]{25}$') + (SELECT count(*) FROM workflow.workflow_hooks WHERE hook_id !~ '^hook_[0-7][0-9A-HJKMNP-TV-Z]{25}$') n`
          )
        ).rows[0].n
      );
      assert.equal(outputJson.invalidIds, 0);
      await sql(client, 'ANALYZE');
      outputJson.sizes = await tableSizes(client);
      if (fixtureMode === 'seed-volume') {
        outputJson.sizeRatios = Object.fromEntries(
          outputJson.sizes.map((r) => [
            r.relname,
            Number(r.total_bytes) / TARGETS[r.relname],
          ])
        );
        await writeFile(
          output + '/full-sizes.json',
          JSON.stringify(
            {
              counts: outputJson.counts,
              sizes: outputJson.sizes,
              ratios: outputJson.sizeRatios,
            },
            null,
            2
          )
        );
        assert.ok(
          Object.values(outputJson.sizeRatios).every(
            (r) => r >= 0.8 && r <= 1.2
          ),
          'full baseline sizes outside required tolerance'
        );
      }
      outputJson.before = await snapshot(db, client);
      await writeFile(
        output + '/baseline-snapshot.json',
        JSON.stringify(outputJson.before, null, 2)
      );
      outputJson.wal = await walSnapshot(client);
      const holder = await client.connect();
      try {
        await holder.query('BEGIN');
        await holder.query('SELECT id FROM workflow.workflow_events LIMIT 1');
        outputJson.locks = await lockSnapshot(client);
        assert.ok(
          outputJson.locks.activity.some(
            (l) =>
              l.pid === holder.processID &&
              l.relation === 'workflow.workflow_events' &&
              l.mode === 'AccessShareLock' &&
              l.granted
          )
        );
      } finally {
        await holder.query('ROLLBACK');
        holder.release();
      }
      const dumpPath =
        output +
        (fixtureMode === 'seed-volume' ? '/full-baseline.dump' : '/smoke.dump');
      await dump(db, dumpPath);
      if (fixtureMode === 'seed-volume') {
        const hash = createHash('sha256');
        for await (const chunk of createReadStream(dumpPath))
          hash.update(chunk);
        const dumpStat = await import('node:fs/promises').then((fs) =>
          fs.stat(dumpPath)
        );
        outputJson.dump = {
          path: dumpPath,
          sha256: hash.digest('hex'),
          bytes: dumpStat.size,
        };
        await writeFile(
          output + '/dump-identity.json',
          JSON.stringify(outputJson.dump, null, 2)
        );
        await client.end();
        client = undefined;
        await sql(admin, 'DROP DATABASE ' + q(db));
        outputJson.seedDatabaseDropped = true;
      }
      await sql(admin, 'CREATE DATABASE ' + q(db2));
      await restore(db2, dumpPath);
      const url2 = new URL(url);
      url2.pathname = '/' + db2;
      restored = new Pool({
        connectionString: url2.toString(),
        application_name: 'slice5a-restored',
        options: '-c TimeZone=UTC',
      });
      outputJson.restoredInspection = await inspectWorkflowDatabase(restored);
      assert.deepEqual(outputJson.restoredInspection, {
        lineage: 'v4.3.10',
        pendingMigrations: 15,
      });
      outputJson.restored = await snapshot(db2, restored);
      // Only the two established PG17 dump/restore cast placements are equivalent.
      const oldPredicate =
        "WHERE ((type)::text = ANY ((ARRAY['step_created'::character varying, 'hook_created'::character varying, 'wait_created'::character varying])::text[]))";
      const restoredPredicate =
        "WHERE ((type)::text = ANY (ARRAY[('step_created'::character varying)::text, ('hook_created'::character varying)::text, ('wait_created'::character varying)::text]))";
      const canonicalize = (text) => {
        assert.ok(
          text.includes(oldPredicate) || text.includes(restoredPredicate)
        );
        return text
          .replace(oldPredicate, 'WHERE <known-v4-predicate>')
          .replace(restoredPredicate, 'WHERE <known-v4-predicate>');
      };
      const b = outputJson.before,
        a = outputJson.restored;
      assert.deepEqual(a.tables, b.tables);
      assert.deepEqual(a.raw, b.raw);
      assert.deepEqual(a.utc, b.utc);
      assert.deepEqual(a.seq, b.seq);
      const beforeDdl = await import('node:fs/promises').then((fs) =>
        fs.readFile(output + '/' + db + '-schema.sql', 'utf8')
      );
      const afterDdl = await import('node:fs/promises').then((fs) =>
        fs.readFile(output + '/' + db2 + '-schema.sql', 'utf8')
      );
      assert.equal(canonicalize(afterDdl), canonicalize(beforeDdl));
      const normalizeFlags = (flags) =>
        flags.map((flag) =>
          flag.relname === 'workflow_events_entity_creation_unique'
            ? { ...flag, definition: canonicalize(flag.definition) }
            : flag
        );
      assert.deepEqual(normalizeFlags(a.flags), normalizeFlags(b.flags));
      outputJson.restoreComparison = {
        raw: true,
        utc: true,
        tableManifest: true,
        sequence: true,
        otherDdlAndIndexFlags: true,
        knownPredicateForms: [
          b.flags.find(
            (f) => f.relname === 'workflow_events_entity_creation_unique'
          ).definition,
          a.flags.find(
            (f) => f.relname === 'workflow_events_entity_creation_unique'
          ).definition,
        ],
      };
      if (fixtureMode === 'smoke' || fixtureMode === 'monitor') {
        // The low-threshold control runs in a separate owned process so its actual
        // nonzero exit and reaping are observable independently of this smoke run.
        const low = child(process.execPath, [fileURLToPath(import.meta.url)], {
          env: {
            ...env,
            SLICE5_MODE: 'low-control',
            SLICE5_ADMIN_URL: url.toString(),
          },
          stdio: ['ignore', 'pipe', 'pipe'],
        });
        let lowStderr = '';
        low.proc.stdout.resume();
        low.proc.stderr.on('data', (c) => (lowStderr += c));
        const controlExit = await low.done;
        assert.deepEqual(controlExit, { code: 1, signal: null });
        const lowResult = JSON.parse(
          await import('node:fs/promises').then((fs) =>
            fs.readFile(output + '/low-control.json', 'utf8')
          )
        );
        assert.equal(lowResult.sqlExit.code, '57014');
        assert.equal(lowResult.exit.signal, 'SIGTERM');
        assert.equal(lowResult.reaped, true);
        outputJson.lowLimit = {
          ...lowResult,
          controlledExit: controlExit.code,
          controlPid: low.proc.pid,
          controlStderr: lowStderr,
        };
        if (fixtureMode === 'monitor') {
          const bad = child(
            process.execPath,
            [fileURLToPath(import.meta.url)],
            {
              env: {
                ...env,
                SLICE5_MODE: 'sample-error-control',
                SLICE5_ADMIN_URL: url.toString(),
              },
              stdio: ['ignore', 'pipe', 'pipe'],
            }
          );
          let stderr = '';
          bad.proc.stdout.resume();
          bad.proc.stderr.on('data', (c) => (stderr += c));
          const exit = await bad.done;
          assert.deepEqual(exit, { code: 1, signal: null });
          const value = JSON.parse(
            await import('node:fs/promises').then((fs) =>
              fs.readFile(output + '/sample-error-control.json', 'utf8')
            )
          );
          assert.equal(value.sqlExit.code, '57014');
          assert.equal(value.exit.signal, 'SIGTERM');
          assert.equal(value.reaped, true);
          assert.equal(value.controllerReason.category, 'sampling_error');
          assert.equal(value.controllerReason.error.code, 73);
          assert.match(
            value.controllerReason.error.stderr,
            /injected sampler failure/
          );
          outputJson.sampleError = {
            ...value,
            controlExit: exit,
            controlPid: bad.proc.pid,
            stderr,
          };
        }
      }
    }
    assert.ok(outputJson.monitor.length > 0);
    outputJson.exit = 0;
  } catch (e) {
    outputJson.error = {
      name: e.name,
      message: e.message,
      code: e.code ?? null,
      signal: e.signal ?? null,
      stack: e.stack,
      controllerReason: firstCancellation ?? null,
    };
    console.error(e);
  } finally {
    if (stopMonitor) await stopMonitor();
    for (const p of children) if (!p.killed) p.kill('SIGTERM');
    while (children.size) await new Promise((r) => setTimeout(r, 20));
    await Promise.allSettled([client?.end(), restored?.end(), admin?.end()]);
    await writeFile(
      output + '/' + fixtureMode + '-result.json',
      JSON.stringify(outputJson, null, 2)
    );
  }
  if (outputJson.exit !== 0) process.exitCode = 1;
}
