// TEST-ONLY entry: source import/diagnostics are not product bootstrap or auth.
// Real product requests use the unchanged production ingress and account host.
import worker, { AccountRepository as Base } from '../src/index';
import { ForgeStorage } from '../src/forge_storage';
import { ClosureJobs } from '../src/closure_jobs';
import { Failure, fail } from '../src/protocol';
import type { Descriptor, ObjectKey } from '../src/core';
interface Env {
  REPOSITORIES: DurableObjectNamespace<Base>; OBJECTS: R2Bucket;
  PRINCIPALS_JSON: string; EXPERIMENTAL_RAW_BACKEND: string; FORGE_BACKEND?: string;
}
export class AccountRepository extends Base {
  constructor(private fixtureCtx: DurableObjectState, private fixtureEnv: Env) {
    super(fixtureCtx, fixtureEnv);
    fixtureCtx.storage.sql.exec('CREATE TABLE IF NOT EXISTS fixture_transform_control(singleton INTEGER PRIMARY KEY CHECK(singleton=1),block_arm_export INTEGER NOT NULL DEFAULT 0,arm_export_calls INTEGER NOT NULL DEFAULT 0)');
    fixtureCtx.storage.sql.exec('INSERT OR IGNORE INTO fixture_transform_control(singleton) VALUES(1)');
  }
  /** Fault-only override: deny the final source arm. Never manufacture peer
   * evidence, permits, validated roots, acknowledgements, or check verdicts. */
  async transformRpc(frame: Parameters<Base['transformRpc']>[0]): Promise<unknown> {
    if (frame.command.action === 'arm_export') {
      const blocked = this.fixtureCtx.storage.transactionSync(() => {
        const sql = this.fixtureCtx.storage.sql;
        const row = [...sql.exec('SELECT block_arm_export FROM fixture_transform_control WHERE singleton=1')][0];
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
      const row = [...sql.exec('SELECT account FROM repository WHERE singleton=1')][0];
      const account = url.searchParams.get('account');
      if (!row || row.account !== account) fail(400, 'fixture_requires_provisioned_account');
      if (url.pathname === '/__fixture/transform-control') {
        if (request.method !== 'POST') fail(405, 'fixture_method');
        const text = await request.text(); if (text.length > 256) fail(400, 'fixture_input_limit');
        const input = JSON.parse(text) as Record<string, unknown>;
        if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).length !== 1 || typeof input.block_arm_export !== 'boolean') fail(400, 'fixture_input_limit');
        this.fixtureCtx.storage.transactionSync(() => sql.exec('UPDATE fixture_transform_control SET block_arm_export=? WHERE singleton=1', input.block_arm_export ? 1 : 0));
        return Response.json([...sql.exec('SELECT block_arm_export,arm_export_calls FROM fixture_transform_control WHERE singleton=1')][0]);
      }
      if (url.pathname === '/__fixture/transform-peek') {
        if (request.method !== 'GET') fail(405, 'fixture_method');
        const exists = (table: string): boolean => [...sql.exec('SELECT name FROM sqlite_master WHERE type=\'table\' AND name=?', table)].length === 1;
        const drafts = exists('transform_drafts') ? [...sql.exec('SELECT id,request_id,phase,run_id,artifact_job,output_job FROM transform_drafts ORDER BY id LIMIT 8')] : [];
        const stage_roots = exists('transform_drafts') && exists('closure_jobs') ? [...sql.exec("SELECT r.owner_kind,r.owner_id,r.kind,r.id,j.phase AS proof_phase,(j.nodes=j.checked_nodes AND j.contexts=j.checked_contexts AND j.obligations=j.checked_obligations) AS complete FROM forge_retained_roots r JOIN transform_drafts d ON d.id=r.owner_id JOIN closure_jobs j ON j.id=CASE r.owner_kind WHEN 'transform-stage-artifact' THEN d.artifact_job ELSE d.output_job END WHERE r.active=1 AND r.owner_kind IN ('transform-stage-artifact','transform-stage-output') AND j.account=? AND j.root_kind=r.kind AND j.root_id=r.id ORDER BY r.owner_kind,r.owner_id LIMIT 16", account)] : [];
        const committed_runs = exists('transform_runs') ? [...sql.exec('SELECT COUNT(*) AS count FROM transform_runs')][0].count : 0;
        const active_pin_roots = [...sql.exec("SELECT COUNT(*) AS count FROM forge_retained_roots WHERE active=1 AND owner_kind IN ('transform-pin','transform-output')")][0].count;
        const views = exists('forge_view_names') ? [...sql.exec("SELECT n.name,n.current_version AS version,json_extract(v.spec,'$.export_allowed') AS export_allowed FROM forge_view_names n JOIN forge_view_versions v ON v.version=n.current_version ORDER BY n.name LIMIT 65")] : [];
        const control = [...sql.exec('SELECT block_arm_export,arm_export_calls FROM fixture_transform_control WHERE singleton=1')][0];
        return Response.json({ drafts, stage_roots, committed_runs, active_pin_roots, views, control });
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
      let status = await jobs.begin('fixture-source-import', input.root);
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
