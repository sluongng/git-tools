import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const executable = resolve(process.argv[2]);
const manifest = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
// Verify the real declared deployment tree and pinned schema, without a
// provider dry-run or inheriting any operator configuration.
const release = new URL("../release/", import.meta.url);
const config = JSON.parse((await readFile(new URL("wrangler.jsonc", release), "utf8"))
  .replace(/^\s*\/\/.*$/gm, ""));
const schema = JSON.parse(await readFile(new URL("../node_modules/wrangler/config-schema.json", import.meta.url), "utf8"));
const assetSchema = schema.definitions.Assets.properties;
assert.equal(config.assets.directory, "./ui-assets");
assert.equal(config.assets.binding, "ASSETS");
assert.equal(config.assets.run_worker_first, true);
for (const key of ["html_handling", "not_found_handling"]) {
  assert.equal(config.assets[key], "none");
  assert.ok(assetSchema[key].enum.includes(config.assets[key]));
}
assert.ok(assetSchema.run_worker_first.anyOf.some(choice => choice.type === "boolean"));
assert.equal(config.workers_dev, false);
assert.equal(config.preview_urls, false);
assert.equal(config.routes, undefined);
assert.equal(config.account_id, undefined);
assert.equal(config.vars, undefined);
assert.equal(config.main, "worker.mjs");
assert.deepEqual(await readFile(new URL("worker.mjs", release)), await readFile(new URL("../worker.mjs", import.meta.url)));
assert.deepEqual(await readFile(new URL("core.wasm", release)), await readFile(new URL("../core.wasm", import.meta.url)));
for (const name of ["index.html", "app.js", "style.css"]) {
  assert.deepEqual(await readFile(new URL(`ui-assets/ui/${name}`, release)),
    await readFile(new URL(`../../crates/tinytree-ui/${name}`, import.meta.url)));
}
assert.deepEqual(await readFile(new URL("ui-assets/ui/tinytree_ui.wasm", release)), await readFile(resolve(process.argv[3])));
execFileSync(process.execPath, ["--check", fileURLToPath(new URL("ui-assets/ui/app.js", release))], { timeout: 30_000 });
const temporary = await mkdtemp(join(process.env.TEST_TMPDIR ?? tmpdir(), "wrangler-smoke-"));
try {
  const home = join(temporary, "home");
  await mkdir(home);
  // No operator home, credentials or provider configuration is inherited.
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    /^(RUNFILES_|TEST_)/.test(key) || key === "PATH" || key === "JAVA_RUNFILES"));
  Object.assign(env, {
    // This nested invocation uses runfiles, not a build action execroot.
    BAZEL_BINDIR: ".",
    HOME: home,
    USERPROFILE: home,
    XDG_CONFIG_HOME: join(home, "config"),
    XDG_CACHE_HOME: join(home, "cache"),
    XDG_STATE_HOME: join(home, "state"),
    WRANGLER_LOG_PATH: join(temporary, "logs"),
    WRANGLER_SEND_METRICS: "false",
    WRANGLER_SEND_ERROR_REPORTS: "false",
    CI: "true",
    NO_COLOR: "1",
  });
  const invoke = argument => execFileSync(executable, [argument], {
    cwd: temporary, env, encoding: "utf8", timeout: 30_000, maxBuffer: 1024 * 1024,
  });
  const version = invoke("--version");
  assert.equal(version.trim(), manifest.devDependencies.wrangler, "execute the pinned Wrangler CLI");
  const help = invoke("--help");
  assert.match(help, /wrangler/);
  assert.match(help, /COMMANDS/);
  assert.match(help, /wrangler deploy/);
  assert.match(help, /--version/);
  console.log(`Wrangler ${version.trim()}: real --version and --help passed`);
} finally {
  await rm(temporary, { recursive: true, force: true });
}
