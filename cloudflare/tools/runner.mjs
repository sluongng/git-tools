import { execFileSync, spawn } from "node:child_process";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";

const directory = fileURLToPath(new URL("../", import.meta.url));
const fixture = process.argv[2];
if (!fixture) throw new Error("declared native contract fixture argument is required");
const temporary = await mkdtemp(join(process.env.TEST_TMPDIR ?? tmpdir(), "native-contract-"));
try {
  const canonical = execFileSync(resolve(fixture), [], {
    encoding: "utf8", timeout: 30_000, maxBuffer: 16 * 1024 * 1024,
  });
  JSON.parse(canonical);
  const fixtureJson = join(temporary, "contract.json");
  await writeFile(fixtureJson, canonical, { mode: 0o600 });
  const child = spawn(process.execPath, ["--test", resolve(directory, "tests/runtime.test.mjs")], {
    stdio: "inherit",
    env: {
      ...process.env,
      TINYTREE_WORKER_BUNDLE: resolve(directory, "worker.mjs"),
      TINYTREE_CORE_WASM: resolve(directory, "core.wasm"),
      TINYTREE_CONTRACT_FIXTURE: fixtureJson,
    },
  });
  process.exitCode = await new Promise((done, reject) => {
    child.on("error", reject);
    child.on("exit", (code, signal) => done(signal ? 1 : code ?? 1));
  });
} finally {
  await rm(temporary, { recursive: true, force: true });
}
