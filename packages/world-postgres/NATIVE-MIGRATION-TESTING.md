# Native migration testing

> **Scope:** These opt-in tests exercise a specific v4-to-v5 drain and upgrade on disposable PostgreSQL 17.11. Ordinary CI without their fixture variables skips them; a skipped test is not migration proof.

The reviewed local runs used Node 24.22.2, this SDK's pnpm 11.24.0 and Vitest 4.1.10. The old runtime was Workflow/core 4.8.15, World 4.5.1 and patched world-postgres 4.3.10; the target was Workflow/core 5.3.0 and World/world-postgres 5.1.0 with the candidate fixes in this source tree. Tests use real PostgreSQL and Graphile. The hand-registered VM bundles in the runtime fixtures are not compiler tests.

## Tests and risks

| Source test | What it exercises | Boundary |
| --- | --- | --- |
| `upgrade-from-v4.test.ts` | Genuine journal preflight, catch-up, restores, refusal and transaction rollback | Only the explicitly configured cases run. |
| `migration-history-native.test.ts` | Real partially complete v4 work survives an OS-process restart, drains on v4, then its terminal history is read on v5 | This is graceful restart and drain, not v5 replay of active v4 histories. |
| `migration-timezone-native.test.ts` | UTC and Sydney readers, known-UTC migration settings, prepared-plan failure and complete rollback comparisons | Its old history dump and exact hash are required. |
| `protocol-isolation-native.test.ts` | Different Graphile task prefixes and same-prefix claim controls | Broker isolation is not mixed-schema rolling safety. |
| `terminal-redelivery-native.test.ts` | Inline and queued success/failure after injected post-commit exceptions and real conflicts | Injected wrapper failures are not network loss. |
| `terminal-network-loss-native.test.ts` | A framed proxy suppresses the actual PostgreSQL COMMIT reply, then Graphile redelivers | Only the selected terminal-write connection is proxied. |
| `wake-redelivery-native.test.ts` | Successful streaming-step continuation after injected publication failure or HTTP 503 | The in-memory stream is not hard-crash durability proof. |
| `migration-volume-native.test.ts` | One volume rollback and one separately restored success plus official no-op | Synthetic rows and local storage; no production window estimate. |

The volume success rehearsal used 103,241 runs, 692,666 steps and 2,469,805 events. The official migration took 8,395 ms locally; the sampled events-table access-exclusive lock had a 6,729 ms observed lower bound, and whole-cluster WAL insert position advanced 260,064,336 bytes. These are not production timings. The three large tables' heap, index and TOAST sizes were measured; whole-database before/after size totals were not captured.

## Running a configured case

Run from the SDK root with the required existing, built artifacts and a private, disposable loopback PostgreSQL container. These tests create databases and write fixtures. They are **not self-contained**: package roots, patched old binaries, compiled target artifacts, `/tmp` dumps and receipts must match the test's expectations. Inspect the test and fixture before running; a missing variable causes a skip. Confirm the selected test reports **one pass and zero skipped**, its child exit assertions pass, and owned containers, volumes, pools and listeners are removed afterward.

The package test command accepts an explicit source file:

```bash
pnpm --filter @workflow/world-postgres test src/migration-history-native.test.ts
pnpm --filter @workflow/world-postgres test src/migration-timezone-native.test.ts
pnpm --filter @workflow/world-postgres test src/protocol-isolation-native.test.ts
pnpm --filter @workflow/world-postgres test src/terminal-redelivery-native.test.ts
pnpm --filter @workflow/world-postgres test src/terminal-network-loss-native.test.ts
pnpm --filter @workflow/world-postgres test src/wake-redelivery-native.test.ts
pnpm --filter @workflow/world-postgres test src/migration-volume-native.test.ts
pnpm --filter @workflow/world-postgres test src/upgrade-from-v4.test.ts
```

Set the relevant variables for the chosen file; `*_ADMIN_URL` is the owned loopback administrator URL, `*_RECEIPT` or `*_RECEIPTS` is an output directory, `*_CONTAINER` is the owned PostgreSQL Docker container, `*_PULSIFY` is the bridge source worktree, and `*_TARGET` is the prepared target package root used by its fixture.

| Test | Required configuration fields |
| --- | --- |
| Upgrade | `WORKFLOW_V4_BOOTSTRAP`, `WORKFLOW_MIGRATION_TEST_URL`; also `WORKFLOW_V4311_BOOTSTRAP` and `WORKFLOW_MIGRATION_DOCKER_CONTAINER` for their respective cases |
| History | `WORKFLOW_NATIVE_HISTORY_ADMIN_URL`, `WORKFLOW_NATIVE_HISTORY_CONTAINER`, `WORKFLOW_SLICE4A_PULSIFY`, `WORKFLOW_SLICE4A_TARGET`, `WORKFLOW_SLICE4A_RECEIPT` |
| Time zone | `WORKFLOW_TZ_ADMIN_URL`, `WORKFLOW_TZ_CONTAINER`, `WORKFLOW_TZ_PULSIFY`, `WORKFLOW_TZ_TARGET`, `WORKFLOW_TZ_RECEIPT` |
| Protocol | `SLICE4C_ADMIN_URL`, `SLICE4C_PULSIFY`, `SLICE4C_TARGET`, `SLICE4C_RECEIPTS` |
| Terminal redelivery | `SLICE4C2_ADMIN_URL`, `SLICE4C2_TARGET`, `SLICE4C2_RECEIPTS` |
| COMMIT-response loss | `SLICE4C3_ADMIN_URL`, `SLICE4C3_TARGET`, `SLICE4C3_RECEIPTS` |
| Wake redelivery | `SLICE4C4_ADMIN_URL`, `SLICE4C4_TARGET`, `SLICE4C4_RECEIPTS` |
| Volume | `SLICE5_ADMIN_URL`, `SLICE5_CONTAINER`, `SLICE5_PULSIFY`, `SLICE5_RECEIPTS`, `SLICE5_MODE` (`volume-rollback` or `volume-success`) |

The time-zone test requires `/tmp/workflow5-slice4a1-proof/v4-history.dump` with SHA-256 `ba1f0f72007b2dc08ce66389ea75711cb0e811688d82c54b4c2fa162f4d712e4`. The volume migration modes require `/tmp/workflow5-slice5b2-proof/run/full-baseline.dump`, 403,275,955 bytes, SHA-256 `05176fa5eb55704c973742eff3c7bb989426fba83cb65c8a6dbf99075da6bed9`, and `/tmp/workflow5-slice5b2-receipt.json`. The fixtures enforce their own additional file and artifact checks. The volume test also depends on the bridge's current lock and child helpers; it is not a portable benchmark.

The local independent SDK/native review passed its stated feasibility scope. Supplementary parsed receipts include `/tmp/workflow5-slice4b1-receipt.json` (SHA-256 `1c27b755f264b70881b72697c20a763b41ecd3911bdb412d65b325c8e3a2e39b`), `/tmp/workflow5-slice5c1-proof/manifest.json` (`5b3b062b4d40249eaada96ba5216c54027efe22dddd34e5e2f2ead00aae2222e`) and `/tmp/workflow5-slice5c2-proof/manifest.json` (`e81ecb15141c16c9fd3451b5398a73db7fc953dc29fd7e199fe2f0a0491bc5a6`). Source tests and actual artifact identities are the reproducible starting point; temporary receipts alone do not establish future results.

## Still required for activation

Establish production's actual journal lineage: the prior read-only journal query was denied. Establish the historical writer time zone rather than assuming current UTC settings prove it. Prove the actual zero-scale deployment and backup/restore plan, and assess representative production volume and unknown stream chunks. Combined application/telemetry behavior and Linux in-flight shutdown remain separate gates. None of these native tests authorizes production changes or an old-binary rollback after v5 writes.
