// TEST-ONLY entry. Never import this module from src/index.ts or deploy it.
// This is a focused ClosureJobHost harness, not product authorization evidence.
import { ClosureJobs, type ClosureBinding, type ClosureJobHost } from '../src/closure_jobs';
import { inspect, type Descriptor, type ObjectKey } from '../src/core';
import { Failure, fail } from '../src/protocol';

interface Env { CLOSURES: DurableObjectNamespace; OBJECTS: R2Bucket }
interface State {
  epoch: number; binding: string; name: string; generation: number;
  head: string | null; attachments: number; fail_attach: number;
}
interface Catalog { kind: number; id: string; size_bytes: number; format_version: number }
const objectPath = (key: ObjectKey) => `closure-fixture/${key.kind}/${key.id}`;

export class ClosureHarness {
  private sql: SqlStorage;
  private gate: Promise<void> | null = null;
  private release: (() => void) | null = null;
  private entered = false;
  constructor(private ctx: DurableObjectState, private env: Env) {
    this.sql = ctx.storage.sql;
    this.sql.exec('CREATE TABLE IF NOT EXISTS harness_state(singleton INTEGER PRIMARY KEY,epoch INTEGER NOT NULL,binding TEXT NOT NULL,name TEXT NOT NULL,generation INTEGER NOT NULL,head TEXT,attachments INTEGER NOT NULL,fail_attach INTEGER NOT NULL)');
    this.sql.exec('CREATE TABLE IF NOT EXISTS harness_catalog(kind INTEGER NOT NULL,id TEXT NOT NULL,size_bytes INTEGER NOT NULL,format_version INTEGER NOT NULL,PRIMARY KEY(kind,id))');
  }
  private one<T>(query: string, ...values: (string | number | null)[]): T | undefined {
    return [...this.sql.exec(query, ...values)][0] as T | undefined;
  }
  private state(): State { const state = this.one<State>('SELECT * FROM harness_state WHERE singleton=1'); if (!state) fail(400, 'fixture_not_initialized'); return state; }
  private jobs(): ClosureJobs {
    const binding = JSON.parse(this.state().binding) as ClosureBinding;
    const host: ClosureJobHost = {
      binding, sql: this.sql,
      transactionSync: callback => this.ctx.storage.transactionSync(callback),
      authorizeCurrentInTx: selected => {
        const state = this.state();
        if (selected.account_id !== binding.account_id || selected.identity.kind !== 'principal' || binding.identity.kind !== 'principal' || selected.identity.principal !== binding.identity.principal || selected.policy_epoch !== state.epoch) fail(403, 'permission_denied');
      },
      describeObjectInTx: key => this.one<Catalog>('SELECT kind,id,size_bytes,format_version FROM harness_catalog WHERE kind=? AND id=?', key.kind, key.id),
      readVerifiedObject: async key => {
        const descriptor = this.one<Catalog>('SELECT kind,id,size_bytes,format_version FROM harness_catalog WHERE kind=? AND id=?', key.kind, key.id);
        const object = await this.env.OBJECTS.get(objectPath(key));
        if (!descriptor || !object) throw new Error('fixture payload unavailable');
        const bytes = new Uint8Array(await object.arrayBuffer());
        const actual = inspect(key.kind, bytes);
        if (actual.kind !== descriptor.kind || actual.id !== descriptor.id || actual.size_bytes !== descriptor.size_bytes || actual.format_version !== descriptor.format_version) throw new Error('fixture payload corrupt');
        // A genuine storage read/verification has happened. While this await is
        // pending another fixture request can revoke the SQL authorization epoch.
        const gate = this.gate;
        if (gate) { this.entered = true; await gate; }
        return { descriptor, bytes };
      },
      attachRootInTx: (selected, root, job) => {
        const state = this.state();
        const expected = JSON.parse(selected.publication);
        if (root.kind !== 3 || expected.name !== state.name || expected.expected_generation !== state.generation || expected.expected_head !== state.head) fail(409, 'aborted');
        this.sql.exec('UPDATE harness_state SET head=?,generation=generation+1,attachments=attachments+1 WHERE singleton=1', root.id);
        // Deliberate AFTER-WRITE failure must roll back this real SQLite update
        // together with ClosureJobs phase/receipt changes in the same transaction.
        if (state.fail_attach) fail(503, 'fixture_attach_failure');
        return { name: state.name, generation: state.generation + 1, head: root.id };
      },
    };
    return new ClosureJobs(host);
  }
  private snapshot(): unknown {
    return {
      state: this.state(), entered: this.entered,
      jobs: [...this.sql.exec('SELECT * FROM closure_jobs ORDER BY id')],
      nodes: [...this.sql.exec('SELECT * FROM closure_nodes ORDER BY job,kind,id')],
      contexts: [...this.sql.exec('SELECT * FROM closure_contexts ORDER BY job,tree,prefix')],
      obligations: [...this.sql.exec('SELECT * FROM closure_obligations ORDER BY job,origin,ordinal')],
      usage: [...this.sql.exec('SELECT * FROM closure_job_usage')],
    };
  }
  async fetch(request: Request): Promise<Response> {
    try {
      const input = await request.json() as Record<string, any>;
      if (input.action === 'setup') {
        if (this.one('SELECT 1 FROM harness_state WHERE singleton=1')) fail(409, 'fixture_already_initialized');
        if (!Array.isArray(input.objects) || input.objects.length > 1024) fail(400, 'fixture_object_limit');
        for (const object of input.objects) {
          const bytes = Uint8Array.from(object.bytes);
          const descriptor = inspect(object.kind, bytes);
          if (descriptor.id !== object.id || descriptor.size_bytes !== object.size_bytes || descriptor.format_version !== (object.format_version ?? 1)) fail(400, 'fixture_descriptor_mismatch');
          await this.env.OBJECTS.put(objectPath(descriptor), bytes);
          this.sql.exec('INSERT INTO harness_catalog(kind,id,size_bytes,format_version) VALUES(?,?,?,?)', descriptor.kind, descriptor.id, descriptor.size_bytes, descriptor.format_version);
        }
        const binding: ClosureBinding = { account_id: 'closure-module-fixture', identity: { kind: 'principal', principal: 'closure-module-fixture' }, policy_epoch: 1, purpose: 'fixture', publication: JSON.stringify({ name: input.main.name, expected_generation: input.main.generation, expected_head: input.main.head }) };
        this.sql.exec('INSERT INTO harness_state VALUES(1,1,?,?,?,?,0,0)', JSON.stringify(binding), input.main.name, input.main.generation, input.main.head);
        this.jobs();
        return Response.json({ initialized: true });
      }
      // Administrative fixture controls remain usable after revocation so a
      // paused read can be released and raw persisted state can be inspected.
      switch (input.action) {
        case 'inspect': return Response.json(this.snapshot());
        case 'fail-attach': this.sql.exec('UPDATE harness_state SET fail_attach=? WHERE singleton=1', input.enabled ? 1 : 0); return Response.json({ configured: true });
        case 'arm-read':
          if (this.gate) fail(409, 'fixture_gate_active');
          this.entered = false;
          this.gate = new Promise(resolve => { this.release = resolve; });
          return Response.json({ armed: true });
        case 'revoke': this.sql.exec('UPDATE harness_state SET epoch=epoch+1 WHERE singleton=1'); return Response.json({ revoked: true });
        case 'release-read': this.release?.(); this.gate = null; this.release = null; return Response.json({ released: true });
      }
      const jobs = this.jobs();
      switch (input.action) {
        case 'begin': return Response.json(await jobs.begin(input.request_id, input.root));
        case 'status': return Response.json(jobs.status(input.job_id));
        case 'advance': return Response.json(await jobs.advance(input.job_id, input.generation));
        case 'publish': return Response.json(jobs.publish(input.job_id, input.generation));
        default: fail(400, 'fixture_unknown_action');
      }
    } catch (error) {
      return Response.json({ error: error instanceof Failure ? error.code : 'fixture_error' }, { status: error instanceof Failure ? error.status : 500 });
    }
  }
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    // All actions are test-control operations. There is deliberately no claim
    // that this callback or local namespace is the product's authentication API.
    return env.CLOSURES.get(env.CLOSURES.idFromName('closure-module-fixture')).fetch(request);
  },
};
