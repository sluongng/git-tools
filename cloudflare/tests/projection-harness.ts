// TEST-ONLY entry: source import/diagnostics are not product bootstrap or auth.
// Real product requests use the unchanged production ingress and account host.
import worker, { AccountRepository as Base } from '../src/index';
import { ForgeStorage } from '../src/forge_storage';
import { PrivateMarkerCatalog } from '../src/private_marker_catalog';
import { ClosureJobs } from '../src/closure_jobs';
import { ForgeGc, type GcBinding } from '../src/forge_gc';
import { Failure, fail } from '../src/protocol';
import type { Descriptor, ObjectKey } from '../src/core';
import { ARCHIVE_SCHEMA } from '../src/archive_schema.generated';
// Bounded disposable-schema diagnostic: never reads an application or KV row.
async function archiveSchemaProbe(ctx: DurableObjectState): Promise<unknown> {
  ctx.storage.transactionSync(() => {
    for (const kind of ['table', 'index']) for (const entry of ARCHIVE_SCHEMA) if (entry.kind === kind) ctx.storage.sql.exec(entry.sql);
  });
  const actual = [...ctx.storage.sql.exec('SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name LIMIT 513')] as { type: string; name: string; tbl_name: string; sql: string | null }[];
  if (actual.length > 512) fail(413, 'fixture_schema_limit');
  const normalize = (sql: string): string => {
    const tokens: string[] = []; let at = 0;
    while (at < sql.length) {
      const c = sql[at]; if (/\s/.test(c)) { at++; continue; }
      if (c === "'" || c === '"' || c === '`' || c === '[') {
        const start = at++, close = c === '[' ? ']' : c; let closed = false;
        while (at < sql.length) if (sql[at++] === close) { if (c !== '[' && sql[at] === close) { at++; continue; } closed = true; break; }
        if (!closed) fail(400, 'fixture_schema_quote'); tokens.push(sql.slice(start, at)); continue;
      }
      const word = /^[A-Za-z_][A-Za-z_0-9]*/.exec(sql.slice(at));
      if (word) { tokens.push(word[0].toUpperCase()); at += word[0].length; } else { tokens.push(c); at++; }
    }
    const pos = tokens[0] === 'CREATE' && tokens[1] === 'UNIQUE' && tokens[2] === 'INDEX' ? 3 : 2;
    if (tokens.slice(pos, pos + 3).join(' ') === 'IF NOT EXISTS') tokens.splice(pos, 3);
    if (tokens.at(-1) === ';') tokens.pop(); return JSON.stringify(tokens);
  };
  const expected = new Map(ARCHIVE_SCHEMA.map(entry => [entry.name, entry]));
  const archive = new Set(['archive_job', 'archive_checkpoint', 'archive_payload_proofs', 'archive_audit_seen']);
  const hash = async (text: string): Promise<string> => Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text))), b => b.toString(16).padStart(2, '0')).join('');
  let ddlBytes = 0; const found = new Set<string>(), mismatches = [];
  for (const row of actual) {
    if (row.sql !== null) ddlBytes += new TextEncoder().encode(row.sql).length;
    if (ddlBytes > 4 * 1024 * 1024) fail(413, 'fixture_schema_limit');
    if (archive.has(row.name)) continue;
    const entry = expected.get(row.name);
    if (row.sql === null && row.type === 'index' && row.name.startsWith('sqlite_autoindex_') && (expected.get(row.tbl_name)?.kind === 'table' || archive.has(row.tbl_name))) continue;
    let reason: string | null = !entry ? 'unknown_object' : row.type !== entry.kind || row.tbl_name !== entry.table ? 'object_identity' : row.sql === null ? 'missing_ddl' : normalize(row.sql) !== normalize(entry.sql) ? 'ddl_mismatch' : null;
    if (entry) found.add(entry.name);
    if (reason) mismatches.push({ type: row.type, name: row.name, table: row.tbl_name, reason,
      actual_hash: row.sql === null ? null : await hash(normalize(row.sql)), expected_hash: entry ? await hash(normalize(entry.sql)) : null });
  }
  for (const entry of ARCHIVE_SCHEMA) if (!found.has(entry.name)) mismatches.push({ type: entry.kind, name: entry.name, table: entry.table, reason: 'missing_object', actual_hash: null, expected_hash: await hash(normalize(entry.sql)) });
  return { actual_objects: actual.length, compiled_objects: ARCHIVE_SCHEMA.length, ddl_bytes: ddlBytes, mismatches };
}
interface Env {
  REPOSITORIES: DurableObjectNamespace<Base>; OBJECTS: R2Bucket;
  PRINCIPALS_JSON: string; EXPERIMENTAL_RAW_BACKEND: string; FORGE_BACKEND?: string;
}
// Fixture-only trusted maintenance driver. No public endpoint accepts owner or
// victim strings in the product; these test-only actions exercise its storage API.
async function storageScenario(ctx: DurableObjectState, bucket: R2Bucket, account: string, request: Request): Promise<unknown> {
  const input = await request.json() as { action: string; object?: Descriptor & { bytes: number[] }; objects?: Descriptor[]; attempt_id?: string };
  const tx = <T>(f: () => T): T => ctx.storage.transactionSync(f);
  const open = (objects = bucket) => new ForgeStorage({ accountId: account, sql: ctx.storage.sql, objects, transactionSync: tx });
  let storage = open();
  const snapshot = () => tx(() => ({ accounting: storage.accountingInTx(), attempts: storage.attemptsInTx().attempts }));
  const failure = async (f: () => unknown | Promise<unknown>) => {
    try { await f(); return null; } catch (error) { if (error instanceof Failure) return { status: error.status, code: error.code }; throw error; }
  };
  if (input.action === 'recover' || input.action === 'delete') {
    if (!input.attempt_id) fail(400, 'fixture_missing_attempt');
    const recovered = input.action === 'recover' ? await storage.recoverAttempt(input.attempt_id) : undefined;
    if (input.action === 'delete') await storage.deleteRetired(input.attempt_id);
    const state = snapshot();
    const d = state.attempts.find(a => a.attempt_id === input.attempt_id)!.descriptor;
    const visible = tx(() => storage.describeInTx(d)) ?? null;
    const read = visible ? Array.from((await storage.readVerifiedObject(d)).bytes) : null;
    return { recovered, ...state, visible, read };
  }
  if (input.action === 'release') {
    if (!input.objects || input.objects.length !== 3) fail(400, 'fixture_input_limit');
    const baseline = snapshot();
    tx(() => { for (const d of input.objects!) storage.reserveInTx('fixture-terminal', 'completed-owner', d); storage.reserveInTx('fixture-live', 'retained-owner', input.objects![0]); });
    const reserved = snapshot();
    const marks = tx(() => storage.reservedKeysInTx(null, 128));
    const releases = [];
    for (let i = 0; i < 4; i++) releases.push(tx(() => storage.releaseReservationsInTx('fixture-terminal', 'completed-owner', 1)));
    const after = snapshot();
    const retained = tx(() => storage.reservedKeysInTx(null, 128));
    return { baseline, reserved, marks, releases, after, retained };
  }
  const d = input.object;
  if (!d || !Array.isArray(d.bytes) || d.bytes.length > 1024 * 1024) fail(400, 'fixture_input_limit');
  const bytes = Uint8Array.from(d.bytes), baseline = snapshot();
  const gate = [...ctx.storage.sql.exec('SELECT active FROM gc_maintenance WHERE singleton=1')][0];
  if (gate?.active) fail(503, 'account_maintenance');
  tx(() => storage.reserveInTx('fixture-terminal', 'completed-owner', d));
  const puts: { key: string; metadata: Record<string, string> | undefined }[] = [];
  const tracked = (put: (key: string, data: Uint8Array, options?: R2PutOptions) => Promise<R2Object | null>, del?: (key: string | string[]) => Promise<void>) => new Proxy(bucket, {
    get(target, property) {
      if (property === 'put') return put;
      if (property === 'delete' && del) return del;
      const value = Reflect.get(target, property); return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  if (input.action === 'put-ack-lost' || input.action === 'put-unobserved') {
    storage = open(tracked(async (key, data, options) => {
      puts.push({ key, metadata: options?.customMetadata });
      if (input.action === 'put-ack-lost') await bucket.put(key, data, options);
      throw new Error('test-only uncertain PUT outcome');
    }));
    const error = await failure(() => storage.stage(d, bytes));
    const head = await bucket.head(puts[0].key);
    return { baseline, error, puts, head: head ? { size: head.size, metadata: head.customMetadata } : null,
      visible: tx(() => storage.describeInTx(d)) ?? null, ...snapshot() };
  }
  if (input.action === 'orphan') {
    const token = await storage.stage(d, bytes);
    tx(() => storage.confirmInTx('fixture-terminal', 'completed-owner', d, token));
    tx(() => storage.releaseReservationsInTx('fixture-terminal', 'completed-owner'));
    const incarnation = tx(() => storage.incarnationInTx(d));
    return { baseline, incarnation, ...snapshot() };
  }
  if (input.action !== 'replace-delayed-delete') fail(400, 'fixture_unknown_storage_action');
  let releaseDelete!: () => void, enteredDelete!: () => void;
  const deleteGate = new Promise<void>(resolve => { releaseDelete = resolve; });
  const deleteEntered = new Promise<void>(resolve => { enteredDelete = resolve; });
  const deletes: string[] = [];
  storage = open(tracked(async (key, data, options) => {
    puts.push({ key, metadata: options?.customMetadata }); return bucket.put(key, data, options);
  }, async key => {
    if (typeof key !== 'string') fail(400, 'fixture_expected_single_delete');
    deletes.push(key); enteredDelete(); await deleteGate; await bucket.delete(key);
    throw new Error('test-only lost DELETE acknowledgement after actual deletion');
  }));
  const oldToken = await storage.stage(d, bytes);
  tx(() => storage.confirmInTx('fixture-terminal', 'completed-owner', d, oldToken));
  const normal = snapshot();
  tx(() => storage.confirmInTx('fixture-terminal', 'completed-owner', d, oldToken));
  const confirmedReplay = snapshot(), original = tx(() => storage.incarnationInTx(d))!;
  const read = await storage.readVerifiedObject(d);
  const reservedRetireError = await failure(() => tx(() => storage.retireInTx(original.attempt_id, original)));
  tx(() => storage.releaseReservationsInTx('fixture-terminal', 'completed-owner'));
  tx(() => storage.retireInTx(original.attempt_id, original));
  // The DELETE has already captured its durable old victim, but has not yet
  // reached R2. Reinstall the identical logical descriptor while it is paused.
  const deleting = failure(() => storage.deleteRetired(original.attempt_id));
  if (!await Promise.race([deleteEntered.then(() => true), deleting.then(() => false)])) fail(400, 'fixture_delete_not_started');
  let replacement: NonNullable<ReturnType<ForgeStorage['incarnationInTx']>>, staleError;
  try {
    tx(() => storage.reserveInTx('fixture-live', 'replacement-owner', d));
    const newToken = await storage.stage(d, bytes);
    tx(() => storage.confirmInTx('fixture-live', 'replacement-owner', d, newToken));
    replacement = tx(() => storage.incarnationInTx(d))!;
    staleError = await failure(() => tx(() => storage.confirmInTx('fixture-live', 'replacement-owner', d, oldToken)));
  } catch (error) { releaseDelete(); await deleting; throw error; }
  releaseDelete(); const deleteError = await deleting;
  const after = snapshot(), reread = await storage.readVerifiedObject(d);
  const heads = await Promise.all(puts.map(async p => { const h = await bucket.head(p.key); return h ? { size: h.size, metadata: h.customMetadata } : null; }));
  return { baseline, normal, confirmedReplay, original, replacement, read: Array.from(read.bytes), reread: Array.from(reread.bytes),
    reservedRetireError, staleError, deleteError, puts, deletes, heads, after };
}
interface CapsuleBackingFault { mode: 'none' | 'before-put' | 'after-put'; failHead: boolean; held?: { key: string; bytes: Uint8Array; options: R2PutOptions | undefined } }
export class AccountRepository extends Base {
  private capsuleFault: CapsuleBackingFault;
  private transformArchiveFault?: { id: string; draft: string; artifact_proof: string; settled: number;
    roots: { owner_kind: string; owner_id: string; kind: number; id: string; active: number }[];
    owners: { run_id: string; kind: string; owner: string }[] };
  constructor(private fixtureCtx: DurableObjectState, private fixtureEnv: Env) {
    // Test-only backing faults: after-PUT also loses its immediate HEAD
    // reconciliation, leaving a genuine ambiguous attempt for restart. Authority remains
    // production code; native object writes and other bucket keys are untouched.
    const fault: CapsuleBackingFault = { mode: 'none', failHead: false };
    const bucket = fixtureEnv.OBJECTS;
    const wrapped = new Proxy(bucket, { get(target, property) {
      if (property === 'put') return async (...args: Parameters<R2Bucket['put']>) => {
        const privateKey = args[0].startsWith('private/v1/');
        if (privateKey && fault.mode === 'before-put') {
          fault.mode = 'none'; const data = args[1];
          if (!(data instanceof Uint8Array) || data.byteLength > 65536) throw new Error('fixture private body shape');
          fault.held = { key: args[0], bytes: Uint8Array.from(data), options: args[2] };
          throw new Error('fixture private PUT unavailable');
        }
        const lose = privateKey && fault.mode === 'after-put'; if (lose) fault.mode = 'none';
        const result = await target.put(...args);
        if (lose) { fault.failHead = true; throw new Error('fixture private PUT acknowledgement lost'); }
        return result;
      };
      if (property === 'head') return async (...args: Parameters<R2Bucket['head']>) => {
        if (args[0].startsWith('private/v1/') && fault.failHead) { fault.failHead = false; throw new Error('fixture private reconciliation HEAD unavailable'); }
        return target.head(...args);
      };
      const value = Reflect.get(target, property); return typeof value === 'function' ? value.bind(target) : value;
    } });
    super(fixtureCtx, { ...fixtureEnv, OBJECTS: wrapped }); this.capsuleFault = fault;
  }
  // Dedicated denial tests create this durable control lazily. Ordinary
  // production-shaped accounts have no test DDL to hide from archive audits.
  private transformControlSnapshot(): { block_arm_export: number; arm_export_calls: number } {
    const sql = this.fixtureCtx.storage.sql;
    if (![...sql.exec("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='fixture_transform_control'")].length) return { block_arm_export: 0, arm_export_calls: 0 };
    return [...sql.exec('SELECT block_arm_export,arm_export_calls FROM fixture_transform_control WHERE singleton=1')][0] as { block_arm_export: number; arm_export_calls: number };
  }
  /** Fault-only override: deny the final source arm. Never manufacture peer
   * evidence, permits, validated roots, acknowledgements, or check verdicts. */
  async transformRpc(frame: Parameters<Base['transformRpc']>[0]): Promise<unknown> {
    if (frame.command.action === 'arm_export') {
      const blocked = this.fixtureCtx.storage.transactionSync(() => {
        const sql = this.fixtureCtx.storage.sql;
        const row = this.transformControlSnapshot();
        if (row?.block_arm_export !== 1) return false;
        sql.exec('UPDATE fixture_transform_control SET arm_export_calls=arm_export_calls+1 WHERE singleton=1');
        return true;
      });
      if (blocked) return { ok: false, status: 503, code: 'unavailable' };
    }
    return super.transformRpc(frame);
  }
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (!url.pathname.startsWith('/__fixture/')) return super.fetch(request);
    try {
      const sql = this.fixtureCtx.storage.sql;
      if (url.pathname === '/__fixture/archive-schema') return Response.json(await archiveSchemaProbe(this.fixtureCtx));
      const row = [...sql.exec('SELECT account FROM repository WHERE singleton=1')][0];
      const account = url.searchParams.get('account');
      if (!row || row.account !== account) fail(400, 'fixture_requires_provisioned_account');
      if (url.pathname === '/__fixture/transform-control') {
        if (request.method !== 'POST') fail(405, 'fixture_method');
        const text = await request.text(); if (text.length > 256) fail(400, 'fixture_input_limit');
        const input = JSON.parse(text) as Record<string, unknown>;
        if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).length !== 1 || typeof input.block_arm_export !== 'boolean') fail(400, 'fixture_input_limit');
        this.fixtureCtx.storage.transactionSync(() => {
          sql.exec('CREATE TABLE IF NOT EXISTS fixture_transform_control(singleton INTEGER PRIMARY KEY CHECK(singleton=1),block_arm_export INTEGER NOT NULL DEFAULT 0,arm_export_calls INTEGER NOT NULL DEFAULT 0)');
          sql.exec('INSERT OR IGNORE INTO fixture_transform_control(singleton) VALUES(1)');
          sql.exec('UPDATE fixture_transform_control SET block_arm_export=? WHERE singleton=1', input.block_arm_export ? 1 : 0);
        });
        return Response.json(this.transformControlSnapshot());
      }
      if (url.pathname === '/__fixture/transform-archive-fault') {
        const text = await request.text(); if (text.length > 256) fail(400, 'fixture_input_limit');
        const input = JSON.parse(text) as { action: string; run_id?: string };
        if (request.method !== 'POST') fail(405, 'fixture_method');
        this.fixtureCtx.storage.transactionSync(() => {
          const archive = [...sql.exec("SELECT name FROM sqlite_schema WHERE type='table' AND name='archive_job'")];
          if (archive.length) {
            const row = [...sql.exec('SELECT body FROM archive_job WHERE singleton=1')][0];
            if (row && !['export_complete', 'complete', 'aborted'].includes(JSON.parse(String(row.body)).phase)) fail(503, 'fixture_archive_active');
          }
          if (input.action === 'restore') {
            const saved = this.transformArchiveFault; if (!saved) fail(400, 'fixture_missing_saved_fault');
            sql.exec('UPDATE transform_runs SET artifact_proof=? WHERE id=?', saved.artifact_proof, saved.id);
            sql.exec('UPDATE transform_drafts SET settled=? WHERE id=?', saved.settled, saved.draft);
            for (const owner of saved.owners) sql.exec('INSERT OR IGNORE INTO transform_pin_owners(run_id,kind,owner) VALUES(?,?,?)', owner.run_id, owner.kind, owner.owner);
            for (const root of saved.roots) sql.exec('UPDATE forge_retained_roots SET active=? WHERE owner_kind=? AND owner_id=? AND kind=? AND id=?', root.active, root.owner_kind, root.owner_id, root.kind, root.id);
            this.transformArchiveFault = undefined; return;
          }
          if (this.transformArchiveFault || !['unsettled', 'missing-pin', 'rebound-proof', 'missing-review-owner'].includes(input.action) || !input.run_id) fail(400, 'fixture_input_limit');
          const run = [...sql.exec('SELECT id,draft,artifact_proof FROM transform_runs WHERE id=?', input.run_id)][0];
          if (!run) fail(400, 'fixture_missing_run');
          const draft = [...sql.exec('SELECT settled FROM transform_drafts WHERE id=?', run.draft)][0];
          if (!draft || draft.settled !== 1) fail(400, 'fixture_requires_settled_run');
          const roots = [...sql.exec("SELECT owner_kind,owner_id,kind,id,active FROM forge_retained_roots WHERE owner_id=? AND owner_kind IN ('transform-pin','transform-output')", input.run_id)] as { owner_kind: string; owner_id: string; kind: number; id: string; active: number }[];
          const owners = [...sql.exec("SELECT run_id,kind,owner FROM transform_pin_owners WHERE run_id=? AND kind='review' ORDER BY owner LIMIT 17", run.id)] as { run_id: string; kind: string; owner: string }[];
          if (owners.length > 16 || (input.action === 'missing-review-owner' && !owners.length)) fail(400, 'fixture_requires_review_owner');
          this.transformArchiveFault = { id: String(run.id), draft: String(run.draft), artifact_proof: String(run.artifact_proof), settled: Number(draft.settled), roots, owners };
          if (input.action === 'unsettled') sql.exec('UPDATE transform_drafts SET settled=0 WHERE id=?', run.draft);
          else if (input.action === 'missing-review-owner') sql.exec('DELETE FROM transform_pin_owners WHERE run_id=? AND kind=? AND owner=?', owners[0].run_id, owners[0].kind, owners[0].owner);
          else if (input.action === 'rebound-proof') sql.exec("UPDATE transform_runs SET artifact_proof='fixture-unrelated-proof' WHERE id=?", run.id);
          else sql.exec("UPDATE forge_retained_roots SET active=0 WHERE owner_kind='transform-pin' AND owner_id=?", run.id);
        });
        return Response.json({ restored: input.action === 'restore' });
      }
      if (url.pathname === '/__fixture/transform-peek') {
        if (request.method !== 'GET') fail(405, 'fixture_method');
        const exists = (table: string): boolean => [...sql.exec('SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?', table)].length === 1;
        const drafts = exists('transform_drafts') ? [...sql.exec('SELECT id,request_id,phase,settled,run_id,artifact_job,output_job FROM transform_drafts ORDER BY id LIMIT 8')] : [];
        const stage_roots = exists('transform_drafts') && exists('closure_jobs') ? [...sql.exec("SELECT r.owner_kind,r.owner_id,r.kind,r.id,j.phase AS proof_phase,(j.nodes=j.checked_nodes AND j.contexts=j.checked_contexts AND j.obligations=j.checked_obligations) AS complete FROM forge_retained_roots r JOIN transform_drafts d ON d.id=r.owner_id JOIN closure_jobs j ON j.id=CASE r.owner_kind WHEN 'transform-stage-artifact' THEN d.artifact_job ELSE d.output_job END WHERE r.active=1 AND r.owner_kind IN ('transform-stage-artifact','transform-stage-output') AND j.account=? AND j.root_kind=r.kind AND j.root_id=r.id ORDER BY r.owner_kind,r.owner_id LIMIT 16", account)] : [];
        const committed_runs = exists('transform_runs') ? [...sql.exec('SELECT COUNT(*) AS count FROM transform_runs')][0].count : 0;
        const active_pin_roots = [...sql.exec("SELECT COUNT(*) AS count FROM forge_retained_roots WHERE active=1 AND owner_kind IN ('transform-pin','transform-output')")][0].count;
        const views = exists('forge_view_names') ? [...sql.exec("SELECT n.name,n.current_version AS version,json_extract(v.spec,'$.export_allowed') AS export_allowed FROM forge_view_names n JOIN forge_view_versions v ON v.version=n.current_version ORDER BY n.name LIMIT 65")] : [];
        const control = this.transformControlSnapshot();
        const terminal_reservations = exists('forge_storage_reservations') && exists('transform_drafts') ? [...sql.exec("SELECT COUNT(*) AS count FROM forge_storage_reservations r JOIN transform_drafts d ON r.owner_id=d.id WHERE r.owner_kind='transform' AND d.phase IN ('committed','aborted')")][0].count : 0;
        return Response.json({ drafts, stage_roots, committed_runs, active_pin_roots, views, control, terminal_reservations });
      }
      if (url.pathname === '/__fixture/private-capsules') {
        const input = await request.json() as { action?: string; mode?: string; capsule?: string; first_sequence?: number; body_hex?: string };
        if (input.action === 'fault') {
          if (!['none', 'before-put', 'after-put'].includes(input.mode ?? '')) fail(400, 'fixture_fault_mode');
          this.capsuleFault.mode = input.mode as typeof this.capsuleFault.mode;
          return Response.json({ mode: this.capsuleFault.mode });
        }
        if (input.action === 'corrupt-marker-page') {
          const page = [...sql.exec('SELECT capsule,first_sequence,body FROM private_marker_pages ORDER BY capsule,first_sequence LIMIT 1')][0];
          if (!page || !(page.body instanceof ArrayBuffer)) fail(400, 'fixture_no_marker_page');
          const original = new Uint8Array(page.body), changed = original.slice(); changed[0] ^= 1;
          sql.exec('UPDATE private_marker_pages SET body=? WHERE capsule=? AND first_sequence=?', changed.buffer, page.capsule, page.first_sequence);
          return Response.json({ capsule: page.capsule, first_sequence: page.first_sequence,
            body_hex: Array.from(original, byte => byte.toString(16).padStart(2, '0')).join('') });
        }
        if (input.action === 'restore-marker-page') {
          if (typeof input.capsule !== 'string' || !/^[0-9a-f]{64}$/.test(input.capsule)
            || !Number.isInteger(input.first_sequence) || input.first_sequence! < 0 || input.first_sequence! > 8191
            || typeof input.body_hex !== 'string' || input.body_hex.length > 256 * 1024 || !/^(?:[0-9a-f]{2})+$/.test(input.body_hex)) fail(400, 'fixture_marker_page');
          const body = Uint8Array.from(input.body_hex.match(/../g)!, byte => parseInt(byte, 16));
          sql.exec('UPDATE private_marker_pages SET body=? WHERE capsule=? AND first_sequence=?', body.buffer, input.capsule, input.first_sequence!);
          return Response.json({ restored: true });
        }
        if (input.action === 'held-put' || input.action === 'late-put') {
          const held = this.capsuleFault.held; if (!held) fail(400, 'fixture_no_held_put');
          let wrote: boolean | null = null;
          if (input.action === 'late-put') wrote = (await this.fixtureEnv.OBJECTS.put(held.key, held.bytes, held.options)) !== null;
          const object = await this.fixtureEnv.OBJECTS.get(held.key);
          return Response.json({ key: held.key, bytes_hex: Array.from(held.bytes, byte => byte.toString(16).padStart(2, '0')).join(''),
            options: held.options, wrote, current: object ? { size: object.size, metadata: object.customMetadata, bytes_hex: Array.from(new Uint8Array(await object.arrayBuffer()), byte => byte.toString(16).padStart(2, '0')).join('') } : null });
        }
        const result: Record<string, unknown> = {};
        for (const table of ['private_capsules', 'private_capsule_heads', 'private_capsule_chunks', 'private_capsule_usage', 'private_marker_pages', 'private_marker_roots', 'private_marker_attempts', 'forge_retained_roots']) {
          if (![...sql.exec("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", table)].length) { result[table] = []; continue; }
          const rows = [...sql.exec(`SELECT * FROM ${table} ORDER BY rowid LIMIT 129`)];
          if (rows.length > 128) fail(413, 'fixture_row_limit'); result[table] = rows;
        }
        // Bounded verified fixture inventory. Product routes do not expose
        // either internal marker keys or the cold SQL catalogue.
        result.private_capsule_cold_chunks = [];
        if ([...sql.exec("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='private_capsule_chunks'")].length) {
          const catalog = new PrivateMarkerCatalog({ accountId: account!, sql });
          const all = []; let after: string | null = null;
          for (let count = 0; count < 129; count++) {
            const next = await catalog.nextAttempt(after); if (!next) break;
            all.push(next); after = next.attempt_id;
          }
          if (all.length > 128) fail(413, 'fixture_row_limit');
          const hot = new Set((result.private_capsule_chunks as { attempt_id: string }[]).map(chunk => chunk.attempt_id));
          result.private_capsule_cold_chunks = all.filter(chunk => !hot.has(chunk.attempt_id));
        }
        result.objects = [...sql.exec('SELECT COUNT(*) AS count FROM objects')];
        return Response.json(result);
      }
      if (url.pathname === '/__fixture/storage') return Response.json(await storageScenario(this.fixtureCtx, this.fixtureEnv.OBJECTS, account!, request));
      if (url.pathname === '/__fixture/gc') {
        const input = await request.json() as { action: string; root?: ObjectKey; gc_id?: string; generation?: number; binding?: GcBinding };
        const tx = <T>(f: () => T): T => this.fixtureCtx.storage.transactionSync(f);
        // Guard-only instance uses actual SQL but has no root/custody/provider
        // capabilities. The facade's private engine remains inaccessible. All
        // real collection progress below goes through the production alarm.
        const guardOnly = () => fail(400, 'fixture_guard_only');
        const collector = () => new ForgeGc({ accountId: account!, sql, transactionSync: tx,
          rootPageInTx: guardOnly, custodyPageInTx: guardOnly,
          closureForRoot: guardOnly, storageForMaintenance: guardOnly });
        if (input.action === 'handoff-seed') {
          const root = input.root;
          if (!root || root.kind !== 3) fail(400, 'fixture_requires_seed_root');
          tx(() => {
            if ([...sql.exec('SELECT active FROM gc_maintenance WHERE singleton=1')][0]?.active) fail(503, 'account_maintenance');
            if (![...sql.exec("SELECT 1 FROM forge_retained_roots WHERE owner_kind='fixture-source' AND owner_id=? AND kind=3 AND id=? AND active=1", root.id, root.id)].length
              || ![...sql.exec("SELECT 1 FROM forge_retained_roots WHERE owner_kind='view' AND kind=3 AND id=? AND active=1", root.id)].length) fail(400, 'fixture_requires_real_view_handoff');
            sql.exec("UPDATE forge_retained_roots SET active=0 WHERE owner_kind='fixture-source' AND owner_id=? AND kind=3 AND id=?", root.id, root.id);
          });
          const storage = new ForgeStorage({ accountId: account!, sql, objects: this.fixtureEnv.OBJECTS, transactionSync: tx });
          let complete = false;
          for (let page = 0; page < 8 && !complete; page++) complete = tx(() => storage.releaseReservationsInTx('fixture-source', root.id, 128)).complete;
          if (!complete) fail(400, 'fixture_handoff_limit');
        } else if (input.action === 'request') {
          tx(() => sql.exec('UPDATE forge_gc_schedule SET requested=1,last_started=0,last_mutation=0 WHERE singleton=1'));
        } else if (input.action === 'tick') {
          // Test clock only: do not wait for platform coalescing/cooldown.
          tx(() => sql.exec('UPDATE forge_gc_schedule SET last_started=0,last_mutation=0 WHERE singleton=1'));
          try { await super.alarm(); } finally { await this.fixtureCtx.storage.deleteAlarm(); }
        } else if (input.action === 'fail-delete-checkpoint') {
          sql.exec("CREATE TRIGGER fixture_gc_delete_checkpoint BEFORE UPDATE ON gc_jobs WHEN NEW.deleted>OLD.deleted BEGIN SELECT RAISE(ABORT,'fixture_gc_delete_checkpoint'); END");
        } else if (input.action === 'clear-delete-checkpoint') {
          sql.exec('DROP TRIGGER fixture_gc_delete_checkpoint');
        } else if (input.action === 'stale-generation') {
          if (!input.gc_id || input.generation === undefined) fail(400, 'fixture_missing_generation');
          const current = [...sql.exec('SELECT generation FROM gc_jobs WHERE id=?', input.gc_id)][0];
          if (!current || current.generation === input.generation) fail(400, 'fixture_requires_stale_generation');
          await collector().advance(input.gc_id, input.generation);
        } else if (input.action === 'stale-binding') {
          if (!input.binding) fail(400, 'fixture_missing_binding');
          tx(() => collector().requireBindingInTx(input.binding!));
        } else if (input.action !== 'inspect') fail(400, 'fixture_unknown_gc_action');
        // Test driver owns alarm delivery timing, but every tick above runs the
        // actual production queue, controller, strict walker and R2 deletion.
        await this.fixtureCtx.storage.deleteAlarm();
        const result: Record<string, unknown> = {};
        for (const table of ['gc_maintenance', 'gc_jobs', 'gc_roots', 'gc_marks', 'forge_gc_schedule']) result[table] = [...sql.exec(`SELECT * FROM ${table}`)];
        result.scratch = [...sql.exec("SELECT id,phase,generation FROM closure_jobs WHERE json_extract(binding,'$[3]')='account-gc'")];
        result.scratch_rows = [...sql.exec("SELECT (SELECT COUNT(*) FROM closure_nodes WHERE job IN(SELECT id FROM closure_jobs WHERE json_extract(binding,'$[3]')='account-gc'))+(SELECT COUNT(*) FROM closure_contexts WHERE job IN(SELECT id FROM closure_jobs WHERE json_extract(binding,'$[3]')='account-gc'))+(SELECT COUNT(*) FROM closure_obligations WHERE job IN(SELECT id FROM closure_jobs WHERE json_extract(binding,'$[3]')='account-gc')) AS count")][0].count;
        result.attempts = [...sql.exec('SELECT a.attempt_id,a.kind,a.id,a.size,a.locator,a.state,CASE WHEN EXISTS(SELECT 1 FROM forge_storage_bindings b WHERE b.kind=a.kind AND b.id=a.id AND b.attempt_id=a.attempt_id) THEN 1 ELSE 0 END AS current FROM forge_storage_attempts a ORDER BY a.attempt_id')];
        result.accounting = new ForgeStorage({ accountId: account!, sql, objects: this.fixtureEnv.OBJECTS, transactionSync: tx }).accountingInTx();
        return Response.json(result);
      }

      if (url.pathname === '/__fixture/inspect') {
        const tables = ['projection_usage', 'projection_jobs', 'projection_frontier', 'projection_selected', 'projection_outputs', 'projection_plan_chunks', 'projection_catalogs', 'closure_job_usage', 'forge_storage_usage', 'forge_upload_usage', 'forge_upload_buffers', 'objects', 'refs'];
        const result: Record<string, unknown> = {};
        for (const table of tables) {
          const exists = [...sql.exec('SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?', table)].length;
          if (!exists) { result[table] = []; continue; }
          result[table] = ['objects', 'projection_frontier', 'projection_selected', 'projection_outputs', 'projection_plan_chunks', 'forge_upload_buffers'].includes(table)
            ? [...sql.exec(`SELECT COUNT(*) AS count FROM ${table}`)] : [...sql.exec(`SELECT * FROM ${table}`)];
        }
        return Response.json(result);
      }
      if (url.pathname !== '/__fixture/seed') fail(400, 'fixture_unknown_action');
      const input = await request.json() as { objects: (Descriptor & { bytes: number[] })[]; root: ObjectKey };
      if (!Array.isArray(input.objects) || input.objects.length > 1024 || input.root.kind !== 3) fail(400, 'fixture_input_limit');
      const storage = new ForgeStorage({ accountId: account!, sql, objects: this.fixtureEnv.OBJECTS, transactionSync: f => this.fixtureCtx.storage.transactionSync(f) });
      if ([...sql.exec('SELECT active FROM gc_maintenance WHERE singleton=1')][0]?.active) fail(503, 'account_maintenance');
      for (const object of input.objects) {
        this.fixtureCtx.storage.transactionSync(() => storage.reserveInTx('fixture-source', input.root.id, object));
        const token = await storage.stage(object, Uint8Array.from(object.bytes));
        this.fixtureCtx.storage.transactionSync(() => storage.confirmInTx('fixture-source', input.root.id, object, token));
      }
      const jobs = new ClosureJobs({
        binding: { account_id: account!, identity: { kind: 'principal', principal: 'fixture-source-import' }, policy_epoch: 1, purpose: 'fixture-source-import', publication: input.root.id },
        sql, transactionSync: f => this.fixtureCtx.storage.transactionSync(f),
        authorizeCurrentInTx: binding => {
          const current = [...sql.exec('SELECT account FROM repository WHERE singleton=1')][0];
          if (!current || current.account !== binding.account_id || binding.publication !== input.root.id) fail(403, 'permission_denied');
        },
        describeObjectInTx: key => storage.describeInTx(key),
        readVerifiedObject: key => storage.readVerifiedObject(key),
        attachRootInTx: (_binding, root, jobId) => {
          const proof = [...sql.exec('SELECT * FROM closure_jobs WHERE id=?', jobId)][0];
          if (!proof || proof.phase !== 'ready' || proof.root_kind !== root.kind || proof.root_id !== root.id
              || proof.nodes !== proof.checked_nodes || proof.contexts !== proof.checked_contexts || proof.obligations !== proof.checked_obligations) fail(400, 'fixture_closure_incomplete');
          sql.exec('INSERT INTO forge_validated_roots(kind,id,proof) VALUES(?,?,?)', root.kind, root.id, JSON.stringify({ validator: 'test-native-closure', root: root.id, job_id: jobId }));
          sql.exec('INSERT INTO forge_retained_roots(owner_kind,owner_id,kind,id,active) VALUES(\'fixture-source\',?,?,?,1)', root.id, root.kind, root.id);
          return { root, job_id: jobId };
        },
      });
      let status = await jobs.begin(`fixture-source-${input.root.id}`, input.root);
      for (let count = 0; count < 64 && !status.complete && !status.error; count++) status = await jobs.advance(status.job_id, status.generation);
      if (!status.complete) fail(400, 'fixture_source_not_ready');
      return Response.json(jobs.publish(status.job_id, status.generation));
    } catch (error) {
      return Response.json({ error: error instanceof Failure ? error.code : 'fixture_error' }, { status: error instanceof Failure ? error.status : 500 });
    }
  }
}
export default {
  fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (!url.pathname.startsWith('/__fixture/')) return worker.fetch(request, env);
    const account = url.searchParams.get('account');
    if (!account || !/^[a-z0-9-]{1,64}$/.test(account)) return Promise.resolve(new Response('invalid fixture account', { status: 400 }));
    return env.REPOSITORIES.get(env.REPOSITORIES.idFromName(`native-account-v1:${account}`)).fetch(request);
  },
};
