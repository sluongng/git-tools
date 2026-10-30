import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const executable = resolve(process.argv[2]);
const manifest = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
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
