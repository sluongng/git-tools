import ts from "typescript";
import { fileURLToPath } from "node:url";
const path = fileURLToPath(new URL("../tsconfig.json", import.meta.url));
const config = ts.readConfigFile(path, ts.sys.readFile);
if (config.error) throw new Error(ts.flattenDiagnosticMessageText(config.error.messageText, "\n"));
const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, fileURLToPath(new URL("../", import.meta.url)), undefined, path);
const program = ts.createProgram(parsed.fileNames, parsed.options);
const diagnostics = [...parsed.errors, ...ts.getPreEmitDiagnostics(program)];
if (diagnostics.length) {
  console.error(ts.formatDiagnosticsWithColorAndContext(diagnostics, {
    getCurrentDirectory: ts.sys.getCurrentDirectory,
    getCanonicalFileName: p => p,
    getNewLine: () => "\n",
  }));
  process.exitCode = 1;
}
