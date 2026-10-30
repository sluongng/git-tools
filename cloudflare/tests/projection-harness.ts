// TEST-ONLY entry: source import/diagnostics are not product bootstrap or auth.
// Real product requests use the unchanged production ingress and account host.
import worker, { AccountRepository as Base } from '../src/index';
import { ForgeStorage } from '../src/forge_storage';
import { ClosureJobs } from '../src/closure_jobs';
import { Failure, fail } from '../src/protocol';
import type { Descriptor, ObjectKey } from '../src/core';
interface Env {
  REPOSITORIES: DurableObjectNamespace; OBJECTS: R2Bucket;
  PRINCIPALS_JSON: string; EXPERIMENTAL_RAW_BACKEND: string; FORGE_BACKEND?: string;
}
export class AccountRepository extends Base {
  constructor(private fixtureCtx: DurableObjectState, private fixtureEnv: Env) { super(fixtureCtx, fixtureEnv); }
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (!url.pathname.startsWith('/__fixture/')) return super.fetch(request);
    try {
      const sql = this.fixtureCtx.storage.sql;
      const row = [...sql.exec('SELECT account FROM repository WHERE singleton=1')][0];
      const account = url.searchParams.get('account');
      if (!row || row.account !== account) fail(400, 'fixture_requires_provisioned_account');
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
