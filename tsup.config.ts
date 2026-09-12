import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    client: "src/client.ts",
    worker: "src/worker.ts",
    workflows: "src/workflows.ts",
    protocol: "src/protocol.ts",
    testing: "src/testing.ts",
    codec: "src/codec.ts",
  },
  format: ["cjs", "esm"],
  outDir: "dist",
  target: "node22",
  platform: "node",
  // Each entry is a self-contained bundle so the published dist/ mirrors the old tsc layout
  // (dist/workflows.js stays resolvable from dist/worker.js).
  splitting: false,
  sourcemap: true,
  clean: true,
  dts: true,
  external: [
    "@temporalio/client",
    "@temporalio/common",
    "@temporalio/testing",
    "@temporalio/worker",
    "@temporalio/workflow",
    "acorn",
    "node:crypto",
    "zod",
  ],
  outExtension: ({ format }) => ({ js: format === "cjs" ? ".js" : ".mjs" }),
  esbuildOptions(options, context) {
    // resolveWorkflowsPath() calls require.resolve at runtime; the CJS bundle has require natively.
    if (context.format === "esm") {
      options.banner = { js: 'import { createRequire as __createRequire } from "node:module"; const require = __createRequire(import.meta.url);' };
    }
    options.logOverride = { "require-resolve-not-external": "silent" };
  },
});
