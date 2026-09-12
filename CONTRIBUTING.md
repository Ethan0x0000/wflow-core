# Contributing

Thanks for your interest in `wflow-core`.

## Development

```bash
pnpm install
pnpm type-check       # tsc for src and tests
pnpm test             # unit tests (excludes the Temporal integration file)
pnpm build            # dual CJS + ESM build via tsup
pnpm test:integration # real execution/replay on TestWorkflowEnvironment
pnpm test:pack        # package smoke test (CJS/ESM entries + workflow bundle purity)
```

The integration suite uses `@temporalio/testing`; it is an optional peer dependency and only needed for `test:integration` and the `wflow-core/testing` subpath.

## Guidelines

- Keep workflow code deterministic: no wall clock, random, network or filesystem access inside `src/workflow.ts` and `src/nodes/*`. Host I/O belongs in activities/adapters.
- Never mutate a published definition in place; publish a new version and use `engine.migrate` for running instances.
- Add tests for new behavior. When you touch interpreter code, also verify that existing scenarios still replay (`pnpm test:integration`).
- Prefer additive, backward-compatible changes to the public API; document any scope decision in `ROADMAP.md`.
- Run `pnpm type-check && pnpm test` before opening a pull request.

## Reporting issues

Please include the definition (redacted if needed), the command/API used, expected vs. actual behavior, and the relevant event sequence or error code.
