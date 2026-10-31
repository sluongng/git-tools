// Build-time only: application DDL is selected from declared TypeScript source,
// never from an archive or arbitrary filesystem discovery.
import ts from 'typescript';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { relative, resolve, sep } from 'node:path';

// Adding a public account entry requires an explicit durable-barrier audit.
export const ARCHIVE_GUARDED_ACCOUNT_ENTRIES = Object.freeze(['fetch', 'alarm', 'transformRpc']);

const MAX_SOURCE_BYTES = 16 * 1024 * 1024;
function sqlTokens(sql) {
  const tokens = [];
  for (let offset = 0; offset < sql.length;) {
    const tail = sql.slice(offset);
    const whitespace = /^\s+/.exec(tail);
    if (whitespace) { offset += whitespace[0].length; continue; }
    if (tail[0] === "'") {
      const literal = /^'(?:[^']|'')*'/.exec(tail);
      if (!literal) throw new Error('archive schema: unterminated SQL literal');
      tokens.push(literal[0]); offset += literal[0].length; continue;
    }
    const word = /^[A-Za-z_][A-Za-z0-9_]*/.exec(tail);
    if (word) { tokens.push(word[0].toUpperCase()); offset += word[0].length; continue; }
    const number = /^\d+(?:\.\d+)?/.exec(tail);
    if (number) { tokens.push(number[0]); offset += number[0].length; continue; }
    if (/^[(),=<>+;*\/-]/.test(tail) && !tail.startsWith('--') && !tail.startsWith('/*')) {
      tokens.push(tail[0]); offset++; continue;
    }
    throw new Error('archive schema: unsupported SQL token');
  }
  if (tokens.at(-1) === ';') tokens.pop();
  if (tokens.includes(';')) throw new Error('archive schema: multiple SQL statements');
  return tokens;
}
function statement(sql) {
  const tokens = sqlTokens(sql);
  if (tokens[0] !== 'CREATE' || !['TABLE', 'INDEX'].includes(tokens[1])
      || tokens.slice(2, 5).join(' ') !== 'IF NOT EXISTS') throw new Error('archive schema: unsupported CREATE form');
  const kind = tokens[1].toLowerCase();
  const name = tokens[5]?.toLowerCase();
  if (!name || !/^[a-z_][a-z0-9_]*$/.test(name)) throw new Error('archive schema: invalid schema identifier');
  let table = name;
  const start = kind === 'table' ? 6 : 8;
  if (kind === 'index') {
    table = tokens[7]?.toLowerCase();
    if (tokens[6] !== 'ON' || !table || !/^[a-z_][a-z0-9_]*$/.test(table)) throw new Error('archive schema: invalid index target');
  }
  if (tokens[start] !== '(' || tokens.at(-1) !== ')') throw new Error('archive schema: incomplete CREATE or unsupported suffix');
  let depth = 0;
  for (let i = start; i < tokens.length; i++) {
    if (tokens[i] === '(') depth++;
    if (tokens[i] === ')') depth--;
    if (depth < 0 || (depth === 0 && i !== tokens.length - 1)) throw new Error('archive schema: unsupported CREATE suffix');
  }
  if (depth !== 0) throw new Error('archive schema: unbalanced CREATE');
  return { kind, name, table, sql: sql.trim().replace(/;\s*$/, ''), canonical: tokens.join(' ') };
}
const ARCHIVE_TABLES = new Set(['archive_job', 'archive_checkpoint', 'archive_payload_proofs', 'archive_audit_seen']);
function internal(item, file) {
  if ([item.name, item.table].some(name => /^(?:sqlite_|__cf_|_cf_)/.test(name))) throw new Error('archive schema: source-defined platform-reserved schema');
  if ([item.name, item.table].some(name => /^archive_/.test(name))) {
    if (file !== 'cloudflare/src/account_archive.ts' || item.kind !== 'table' || item.table !== item.name || !ARCHIVE_TABLES.has(item.name)) throw new Error('archive schema: unrecognized archive control schema');
    return true;
  }
  return false;
}
function extract(source, file) {
  const parsed = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  if (parsed.parseDiagnostics.length) throw new Error('archive schema: invalid declared TypeScript');
  const statements = [];
  const walk = node => {
    if (ts.isClassDeclaration(node) && node.name?.text === 'AccountRepository') {
      for (const member of node.members) {
        if (!ts.isMethodDeclaration(member) || member.modifiers?.some(m => m.kind === ts.SyntaxKind.PrivateKeyword)) continue;
        if (!ARCHIVE_GUARDED_ACCOUNT_ENTRIES.includes(member.name.getText(parsed))) throw new Error('archive schema: public account RPC requires archive barrier integration');
      }
    }
    if (ts.isTemplateExpression(node) && /^\s*(?:CREATE|ALTER|DROP)\b/i.test(node.head.text)) throw new Error('archive schema: computed DDL unsupported');
    if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
      const value = node.text;
      if (/^\s*(?:CREATE|ALTER|DROP)\b/i.test(value)) {
        let expression = node;
        while (ts.isParenthesizedExpression(expression.parent)) expression = expression.parent;
        const parent = expression.parent;
        if ((ts.isBinaryExpression(parent) && parent.operatorToken.kind === ts.SyntaxKind.PlusToken)
            || ts.isTaggedTemplateExpression(parent)) throw new Error('archive schema: computed DDL unsupported');
        // Lexical validators compare standalone grammar tokens such as
        // tokens[0] === 'CREATE'. They contain no schema definition. A bare
        // verb passed to SQL exec is still invalid, and concatenated/template
        // DDL has already been rejected above rather than silently omitted.
        const executed = ts.isCallExpression(parent) && ts.isPropertyAccessExpression(parent.expression)
          && parent.expression.name.text === 'exec' && parent.arguments.includes(expression);
        if (/^\s*(?:CREATE|ALTER|DROP)\s*$/i.test(value) && !executed) return;
        if (!/^\s*CREATE\b/i.test(value)) throw new Error('archive schema: schema mutation unsupported');
        const item = statement(value);
        if (!internal(item, file)) statements.push(item);
      }
    }
    ts.forEachChild(node, walk);
  };
  walk(parsed);
  return statements;
}
function frame(hash, value) {
  const size = Buffer.alloc(8); size.writeBigUInt64LE(BigInt(value.length)); hash.update(size); hash.update(value);
}
export async function archiveSchema({ root, sources, wasm }) {
  if (!Array.isArray(sources) || sources.length === 0 || sources.length > 256) throw new Error('archive schema: declared source list required');
  const declared = new Map();
  for (const input of sources) {
    const file = resolve(root, input);
    const name = relative(root, file).split(sep).join('/');
    if (!/^cloudflare\/src\/(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+\.ts$/.test(name)) throw new Error('archive schema: source is not a declared application TypeScript path');
    if (name === 'cloudflare/src/archive_schema.generated.d.ts') continue;
    if (declared.has(name)) throw new Error('archive schema: duplicate declared input');
    declared.set(name, file);
  }
  if (!declared.has('cloudflare/src/index.ts')) throw new Error('archive schema: application entry missing');
  const hash = createHash('sha256'); hash.update('tinytree-cloudflare-account-archive-build-v1\0');
  const inventory = new Map(); let sourceBytes = 0;
  for (const [name, file] of [...declared].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) {
    const bytes = await readFile(file); sourceBytes += bytes.length;
    if (sourceBytes > MAX_SOURCE_BYTES) throw new Error('archive schema: source byte limit');
    frame(hash, Buffer.from(name)); frame(hash, bytes);
    for (const item of extract(bytes.toString('utf8'), name)) {
      const previous = inventory.get(item.name);
      if (previous && previous.canonical !== item.canonical) throw new Error(`archive schema: inconsistent duplicate ${item.name}`);
      if (!previous) inventory.set(item.name, item);
    }
  }
  frame(hash, Buffer.from('core.wasm')); frame(hash, await readFile(wasm));
  const schema = [...inventory.values()].sort((a, b) => (a.kind === b.kind ? 0 : a.kind === 'table' ? -1 : 1) || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const tables = new Set(schema.filter(item => item.kind === 'table').map(item => item.name));
  if (tables.size === 0 || schema.length > 512 || schema.some(item => item.kind === 'index' && !tables.has(item.table))) throw new Error('archive schema: incomplete application inventory');
  const applicationSchema = schema.map(({ canonical, ...item }) => item);
  frame(hash, Buffer.from('archive-schema.mjs')); frame(hash, await readFile(new URL(import.meta.url)));
  frame(hash, Buffer.from(JSON.stringify(applicationSchema)));
  return { buildId: hash.digest('hex'), schema: applicationSchema };
}
