/** Account/member/replay authority. The embedding authenticates every call and
 * binds this host to exactly one account Durable Object. R2 contains bytes only. */
import { requireNoArmedExportInTx } from './forge_transforms';
import { normalizeForgeAccountCommand, type ForgeCall, type Descriptor } from './core';
import { fail } from './protocol';
import type { PolicySnapshot } from './forge_views';

export interface ForgeMember {
  principal: string;
  can_write: boolean;
  can_review: boolean;
  is_admin: boolean;
  roles: string[];
}
export interface CanonicalAccountObject extends Descriptor { bytes: number[] }
export interface AccountCommand {
  operation: 1 | 2 | 3;
  fingerprint: string | null;
  member?: ForgeMember;
  expected_epoch?: number;
  genesis?: { head: string; objects: CanonicalAccountObject[] };
}
export interface ForgeAccountHost {
  /** Trusted current authenticated values, not fields extracted from JSON. */
  accountId: string;
  principal: string | null;
  canProvision: boolean;
  sql: SqlStorage;
  transactionSync<T>(callback: () => T): T;
  /** Verify native canonical bytes and confirm durable R2 puts; no SQL claim. */
  stageGenesis(objects: CanonicalAccountObject[]): Promise<void>;
  /** Same SQL transaction: reject an existing unowned raw repository, and claim
   * repository + staged object descriptors + canonical main generation 1. */
  claimRepositoryInTx(accountId: string, objects: CanonicalAccountObject[], head: string): void;
  /** Read authoritative canonical main from the same SQL transaction. */
  currentMainInTx(): { head: string; generation: number };
}
interface AccountRow {
  account_id: string;
  owner: string;
  policy_epoch: number;
  public_view: string | null;
}
interface MemberRow { value: string }
interface ReceiptRow { fingerprint: string; result: string }
const MAX_MEMBERS = 64;
const MAX_RECEIPTS = 4096;
const encoder = new TextEncoder();

/** Raw-route fencing without an actor or schema creation. Any account row
 * disables raw authority; a missing table means this DO has not been claimed. */
export function hasForgeAccount(sql: SqlStorage): boolean {
  if ([...sql.exec("SELECT 1 FROM sqlite_master WHERE type='table' AND name='forge_account'")].length === 0) return false;
  return [...sql.exec('SELECT 1 FROM forge_account WHERE singleton=1')].length !== 0;
}

export class ForgeAccounts {
  constructor(private readonly host: ForgeAccountHost) {
    for (const statement of [
      'CREATE TABLE IF NOT EXISTS forge_account (singleton INTEGER PRIMARY KEY CHECK(singleton=1), account_id TEXT NOT NULL UNIQUE, owner TEXT NOT NULL, policy_epoch INTEGER NOT NULL CHECK(policy_epoch>=1), public_view TEXT)',
      'CREATE TABLE IF NOT EXISTS forge_members (account_id TEXT NOT NULL, principal TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(account_id,principal))',
      'CREATE TABLE IF NOT EXISTS forge_account_receipts (account_id TEXT NOT NULL, principal TEXT NOT NULL, request_id TEXT NOT NULL, fingerprint TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(account_id,principal,request_id))',
      'CREATE TABLE IF NOT EXISTS forge_bootstrap (singleton INTEGER PRIMARY KEY CHECK(singleton=1), account TEXT NOT NULL, sealed INTEGER NOT NULL CHECK(sealed IN(0,1)), head TEXT, import_id TEXT, job_id TEXT)',
    ]) host.sql.exec(statement);
  }
  private one<T>(query: string, ...bindings: (string | number | null)[]): T | undefined {
    return [...this.host.sql.exec(query, ...bindings)][0] as T | undefined;
  }
  private accountInTx(): AccountRow | undefined {
    const row = this.one<AccountRow>('SELECT account_id,owner,policy_epoch,public_view FROM forge_account WHERE singleton=1');
    if (row && row.account_id !== this.host.accountId) fail(403, 'permission_denied');
    return row;
  }
  /** Host-side fencing only, not an authorization grant or public operation. */
  hasAccountInTx(): boolean { return this.accountInTx() !== undefined; }
  private authorizeInTx(action: 'member' | 'admin' | 'write' | 'review', principal: string | null = this.host.principal): AccountRow {
    if (principal === null) fail(401, 'unauthenticated');
    const row = this.accountInTx();
    if (!row) fail(403, 'permission_denied');
    if (row.owner === principal) return row;
    const member = this.one<MemberRow>('SELECT value FROM forge_members WHERE account_id=? AND principal=?', row.account_id, principal);
    if (!member) fail(403, 'permission_denied');
    const grant = JSON.parse(member.value) as ForgeMember;
    if ((action === 'admin' && grant.is_admin !== true)
        || (action === 'write' && grant.is_admin !== true && grant.can_write !== true)
        || (action === 'review' && grant.is_admin !== true && grant.can_review !== true)) fail(403, 'permission_denied');
    return row;
  }
  /** Trusted review embedding only: alternate subjects come from its persisted
   * decision records, never a public actor/grant selector. */
  authorityInTx(action: 'read' | 'write' | 'review', expectedEpoch?: number, principal: string | null = this.host.principal): { epoch: number; is_admin: boolean } {
    const row = this.authorizeInTx(action === 'read' ? 'member' : action, principal);
    if (expectedEpoch !== undefined && row.policy_epoch !== expectedEpoch) fail(409, 'aborted');
    if (row.owner === principal) return { epoch: row.policy_epoch, is_admin: true };
    const saved = this.one<MemberRow>('SELECT value FROM forge_members WHERE account_id=? AND principal=?', row.account_id, principal);
    return { epoch: row.policy_epoch, is_admin: saved !== undefined && (JSON.parse(saved.value) as ForgeMember).is_admin === true };
  }
  private wire(row: AccountRow): Record<string, unknown> {
    return { account_id: row.account_id, monorepo_id: `forge-${row.account_id}`, policy_epoch: row.policy_epoch,
      public_view: row.public_view === null ? null : JSON.parse(row.public_view) };
  }
  private snapshot(row: AccountRow): Record<string, unknown> {
    const main = this.host.currentMainInTx();
    if (!/^[0-9a-f]{64}$/.test(main.head) || main.head === '0'.repeat(64)
        || !Number.isSafeInteger(main.generation) || main.generation < 1) fail(503, 'failed_precondition');
    return { ...this.wire(row), main_revision: Array.from(main.head.match(/../g)!, byte => parseInt(byte, 16)), main_generation: main.generation };
  }
  /** Internal shared authority for view/read embeddings. A snapshot is not a
   * continuing grant: callers must rederive it inside every authority tx. */
  policySnapshotInTx(): PolicySnapshot {
    const row = this.accountInTx();
    if (!row) fail(403, 'permission_denied');
    let member: ForgeMember | undefined;
    if (this.host.principal !== null) {
      const saved = this.one<MemberRow>('SELECT value FROM forge_members WHERE account_id=? AND principal=?', row.account_id, this.host.principal);
      if (saved) member = JSON.parse(saved.value) as ForgeMember;
    }
    let publicView: string | null = null;
    if (row.public_view !== null) {
      const bytes: unknown = JSON.parse(row.public_view);
      if (!Array.isArray(bytes) || bytes.length !== 32 || !bytes.every(v => Number.isInteger(v) && v >= 0 && v <= 255)) fail(503, 'failed_precondition');
      publicView = bytes.map(v => v.toString(16).padStart(2, '0')).join('');
    }
    return { accountId: row.account_id, principal: this.host.principal, epoch: row.policy_epoch, publicView,
      grant: { is_admin: row.owner === this.host.principal || member?.is_admin === true,
        member_roles: row.owner === this.host.principal ? [] : member?.roles ?? null } };
  }
  requireAdminInTx(): PolicySnapshot {
    this.authorizeInTx('admin');
    return this.policySnapshotInTx();
  }
  /** Derive ordinary write authority from current account membership. Provider
   * admission/operator status is deliberately not an account write grant. */
  requireWriterInTx(): PolicySnapshot {
    this.authorizeInTx('write');
    return this.policySnapshotInTx();
  }
  /** Immutable owner authority, not an admin/member grant. Missing bootstrap
   * state is not an untouched account: provisioning alone creates it. */
  requireOwnerInTx(): { account_id: string; owner: string; policy_epoch: number; main: { head: string; generation: number }; bootstrap_sealed: boolean } {
    if (this.host.principal === null) fail(401, 'unauthenticated');
    const row = this.accountInTx();
    if (!row || row.owner !== this.host.principal) fail(403, 'permission_denied');
    const state = this.one<{ account: string; sealed: number }>('SELECT account,sealed FROM forge_bootstrap WHERE singleton=1');
    if (!state || state.account !== row.account_id || ![0, 1].includes(Number(state.sealed))) fail(400, 'failed_precondition');
    return { account_id: row.account_id, owner: row.owner, policy_epoch: row.policy_epoch,
      main: this.host.currentMainInTx(), bootstrap_sealed: Number(state.sealed) === 1 };
  }
  /** Every canonical-main publisher must call this in its existing publication
   * transaction. Monotonic only; there is deliberately no reset/reopen API. */
  sealBootstrapInTx(head: string, importId: string | null = null, jobId: string | null = null): void {
    if (!/^[0-9a-f]{64}$/.test(head) || head === '0'.repeat(64)) fail(503, 'failed_precondition');
    const row = this.accountInTx();
    const state = this.one<{ account: string; sealed: number }>('SELECT account,sealed FROM forge_bootstrap WHERE singleton=1');
    if (!row || !state || state.account !== row.account_id) fail(400, 'failed_precondition');
    // Preserve the first sealing identity on all subsequent publications.
    this.host.sql.exec('UPDATE forge_bootstrap SET sealed=1,head=?,import_id=?,job_id=? WHERE singleton=1 AND sealed=0', head, importId, jobId);
  }
  accountWireInTx(): Record<string, unknown> {
    const row = this.accountInTx(); if (!row) fail(403, 'permission_denied');
    return this.wire(row);
  }
  bumpEpochInTx(expected: number): number {
    requireNoArmedExportInTx(this.host.sql, this.host.accountId);
    if (!Number.isSafeInteger(expected) || expected < 1 || expected >= Number.MAX_SAFE_INTEGER) fail(429, 'resource_exhausted');
    const updated = this.one<{ policy_epoch: number }>('UPDATE forge_account SET policy_epoch=policy_epoch+1 WHERE singleton=1 AND policy_epoch=? RETURNING policy_epoch', expected);
    if (!updated) fail(409, 'aborted');
    return Number(updated.policy_epoch);
  }
  setPublicViewInTx(version: string | null): void {
    if (version !== null && !/^[0-9a-f]{64}$/.test(version)) fail(503, 'failed_precondition');
    const value = version === null ? null : JSON.stringify(Array.from(version.match(/../g)!, pair => parseInt(pair, 16)));
    this.host.sql.exec('UPDATE forge_account SET public_view=? WHERE singleton=1', value);
  }
  replayInTx(requestId: string, fingerprint: string): unknown | undefined {
    if (this.host.principal === null) fail(401, 'unauthenticated');
    const receipt = this.one<ReceiptRow>('SELECT fingerprint,result FROM forge_account_receipts WHERE account_id=? AND principal=? AND request_id=?', this.host.accountId, this.host.principal, requestId);
    if (!receipt) return undefined;
    if (receipt.fingerprint !== fingerprint) fail(400, 'invalid_argument');
    return JSON.parse(receipt.result);
  }
  finishInTx(requestId: string, fingerprint: string, result: unknown): unknown {
    if (this.host.principal === null) fail(401, 'unauthenticated');
    const count = this.one<{ n: number }>('SELECT COUNT(*) AS n FROM forge_account_receipts WHERE account_id=?', this.host.accountId)!;
    if (Number(count.n) >= MAX_RECEIPTS) fail(429, 'resource_exhausted');
    const encoded = JSON.stringify(result);
    if (encoder.encode(encoded).byteLength > 64 * 1024) fail(413, 'resource_exhausted');
    this.host.sql.exec('INSERT INTO forge_account_receipts(account_id,principal,request_id,fingerprint,result) VALUES(?,?,?,?,?)', this.host.accountId, this.host.principal, requestId, fingerprint, encoded);
    return result;
  }
  async handle(call: ForgeCall): Promise<{ state: 'complete'; result: unknown }> {
    if (call.account_id !== this.host.accountId) fail(403, 'permission_denied');
    if (this.host.principal === null) fail(401, 'unauthenticated');
    if (encoder.encode(this.host.principal).byteLength < 1 || encoder.encode(this.host.principal).byteLength > 128
        || new TextDecoder('utf-8', { fatal: true, ignoreBOM: false }).decode(encoder.encode(this.host.principal)) !== this.host.principal
        || /[\u0000-\u001f\u007f-\u009f]/u.test(this.host.principal)) fail(401, 'unauthenticated');
    if (![1, 2, 3].includes(call.operation)) fail(501, 'unimplemented');
    let command: AccountCommand;
    try { command = normalizeForgeAccountCommand({ account_id: call.account_id, operation: call.operation, view: call.view, payload_json: call.payload_json }); }
    catch (error) {
      if (error instanceof Error && (error.message.startsWith('forge limit:') || error.message.endsWith('limit'))) fail(413, 'resource_exhausted');
      fail(400, 'invalid_argument');
    }
    if (command.operation !== call.operation) fail(503, 'failed_precondition');
    if (command.operation === 2) {
      return this.host.transactionSync(() => ({ state: 'complete', result: this.snapshot(this.authorizeInTx('member')) }));
    }
    if (!/^[A-Za-z0-9_.-]{1,128}$/.test(call.request_id)) fail(400, 'invalid_argument');
    const fingerprint = command.fingerprint;
    if (typeof fingerprint !== 'string' || !/^[0-9a-f]{64}$/.test(fingerprint)) fail(503, 'failed_precondition');
    if (command.operation === 1) {
      if (!this.host.canProvision) fail(403, 'permission_denied');
      // Check ownership and authorization before even historical replay. A new
      // request against an existing account returns its current snapshot.
      const early = this.host.transactionSync(() => {
        const row = this.accountInTx();
        if (row && row.owner !== this.host.principal) fail(403, 'permission_denied');
        const replay = this.replayInTx(call.request_id, fingerprint);
        if (replay !== undefined) return replay;
        if (row) return this.finishInTx(call.request_id, fingerprint, this.snapshot(row));
        return undefined;
      });
      if (early !== undefined) return { state: 'complete', result: early };
      const genesis = command.genesis;
      if (!genesis) fail(503, 'failed_precondition');
      await this.host.stageGenesis(genesis.objects);
      return this.host.transactionSync(() => {
        if (!this.host.canProvision) fail(403, 'permission_denied');
        let row = this.accountInTx();
        if (row && row.owner !== this.host.principal) fail(403, 'permission_denied');
        const replay = this.replayInTx(call.request_id, fingerprint);
        if (replay !== undefined) return { state: 'complete', result: replay };
        if (!row) {
          this.host.claimRepositoryInTx(this.host.accountId, genesis.objects, genesis.head);
          this.host.sql.exec('INSERT INTO forge_account(singleton,account_id,owner,policy_epoch,public_view) VALUES(1,?,?,1,NULL)', this.host.accountId, this.host.principal);
          this.host.sql.exec('INSERT INTO forge_bootstrap(singleton,account,sealed) VALUES(1,?,0)', this.host.accountId);
          row = this.accountInTx()!;
        }
        return { state: 'complete', result: this.finishInTx(call.request_id, fingerprint, this.snapshot(row)) };
      });
    }
    return this.host.transactionSync(() => {
      const row = this.authorizeInTx('admin');
      // Current admin authorization precedes historical replay, but the old
      // expected epoch does not invalidate a previously committed receipt.
      const replay = this.replayInTx(call.request_id, fingerprint);
      if (replay !== undefined) return { state: 'complete', result: replay };
      if (command.expected_epoch !== row.policy_epoch) fail(409, 'aborted');
      const member = command.member;
      if (!member) fail(503, 'failed_precondition');
      if (member.principal === row.owner) fail(400, 'invalid_argument');
      const current = this.one<MemberRow>('SELECT value FROM forge_members WHERE account_id=? AND principal=?', row.account_id, member.principal);
      const encoded = JSON.stringify(member);
      if (!current) {
        const count = this.one<{ n: number }>('SELECT COUNT(*) AS n FROM forge_members WHERE account_id=?', row.account_id)!;
        if (Number(count.n) >= MAX_MEMBERS) fail(429, 'resource_exhausted');
      }
      if (current?.value !== encoded) {
        if (!Number.isSafeInteger(row.policy_epoch) || row.policy_epoch >= Number.MAX_SAFE_INTEGER) fail(429, 'resource_exhausted');
        this.host.sql.exec('INSERT INTO forge_members(account_id,principal,value) VALUES(?,?,?) ON CONFLICT(account_id,principal) DO UPDATE SET value=excluded.value', row.account_id, member.principal, encoded);
        row.policy_epoch = this.bumpEpochInTx(row.policy_epoch);
      }
      return { state: 'complete', result: this.finishInTx(call.request_id, fingerprint, this.wire(row)) };
    });
  }
}
