import { getWriteDb } from '../pipeline/db';
import { loadSchemaSql } from './schema';

export async function runMigrations() {
  const { writeSql, readSql } = loadSchemaSql();
  const sql = getWriteDb();

  await sql.begin(async (tx) => {
    await tx.unsafe(writeSql);
    await tx.unsafe(readSql);
  });
}
