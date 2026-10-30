// CLI is opt-in; there is deliberately no automatic deployment target.
import { fileURLToPath } from "node:url";
process.argv.push("--config", fileURLToPath(new URL("../wrangler.jsonc", import.meta.url)));
await import("wrangler/bin/wrangler.js");
