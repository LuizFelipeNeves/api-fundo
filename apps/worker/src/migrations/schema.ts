import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

export function loadSchemaSql() {
  const root = process.cwd();
  const writePath = resolve(root, 'database/write-repositories/schema.sql');
  const readPath = resolve(root, 'database/read-repositories/schema.sql');
  const writeSql = readFileSync(writePath, 'utf-8');
  const readSql = readFileSync(readPath, 'utf-8');
  return { writeSql, readSql };
}
