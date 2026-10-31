/** Build-time virtual module. No incoming archive supplies executable SQL. */
export interface ArchiveSchemaEntry {
  readonly kind: 'table' | 'index';
  readonly name: string;
  readonly table: string;
  readonly sql: string;
}
export declare const ARCHIVE_BUILD_ID: string;
export declare const ARCHIVE_SCHEMA: readonly ArchiveSchemaEntry[];
