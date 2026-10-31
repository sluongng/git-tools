import { build } from "esbuild";
import { copyFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { archiveSchema } from "./archive-schema.mjs";

const root = process.env.JS_BINARY__EXECROOT ?? process.cwd();
const [entryInput, wasmInput, workerInput, wasmOutputInput, ...sources] = process.argv.slice(2);
const [entry, wasm, workerOutput, wasmOutput] = [entryInput, wasmInput, workerInput, wasmOutputInput].map(p => p && resolve(root, p));
if (!wasmOutput) throw new Error("expected entry, wasm input, worker output, wasm output");
const archive = await archiveSchema({ root, sources, wasm });
await mkdir(dirname(workerOutput), { recursive: true });
await build({
  entryPoints: [entry],
  outfile: workerOutput,
  bundle: true,
  format: "esm",
  platform: "neutral",
  target: "es2022",
  external: ["./core.wasm", "cloudflare:workers"],
  legalComments: "none",
  sourcemap: false,
  plugins: [{
    name: "compiled-account-archive-schema",
    setup(plugin) {
      plugin.onResolve({ filter: /(?:^|\/)archive_schema\.generated$/ }, () => ({ path: "archive-schema", namespace: "compiled-archive" }));
      plugin.onLoad({ filter: /.*/, namespace: "compiled-archive" }, () => ({
        loader: "js", contents: `export const ARCHIVE_BUILD_ID=${JSON.stringify(archive.buildId)};export const ARCHIVE_SCHEMA=Object.freeze(${JSON.stringify(archive.schema)}.map(row=>Object.freeze(row)));`,
      }));
    },
  }],
});
await copyFile(wasm, wasmOutput);
