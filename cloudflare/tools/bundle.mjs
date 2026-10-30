import { build } from "esbuild";
import { copyFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const root = process.env.JS_BINARY__EXECROOT ?? process.cwd();
const [entry, wasm, workerOutput, wasmOutput] = process.argv.slice(2).map(p => resolve(root, p));
if (!wasmOutput) throw new Error("expected entry, wasm input, worker output, wasm output");
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
});
await copyFile(wasm, wasmOutput);
