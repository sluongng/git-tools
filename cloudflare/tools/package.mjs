// Assemble only declared build inputs into the complete relocatable deployment
// tree. This tool selects no account, credentials, route, or provider resources.
import { copyFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';

const root = process.env.JS_BINARY__EXECROOT ?? process.cwd();
const args = process.argv.slice(2);
if (args.length !== 8) throw new Error('expected worker, core, config, HTML, JS, CSS, UI Wasm and release directory');
const [worker, core, config, html, js, css, uiWasm, output] = args.map(value => resolve(root, value));
const files = [
  [worker, 'worker.mjs'], [core, 'core.wasm'], [config, 'wrangler.jsonc'],
  [html, 'ui-assets/ui/index.html'], [js, 'ui-assets/ui/app.js'],
  [css, 'ui-assets/ui/style.css'], [uiWasm, 'ui-assets/ui/tinytree_ui.wasm'],
];
for (const [source, name] of files) {
  const target = resolve(output, name);
  await mkdir(dirname(target), { recursive: true });
  await copyFile(source, target);
}
