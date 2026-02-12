import postgres from 'postgres';

export type Sql = ReturnType<typeof postgres>;

// Re-export from db/index for backwards compatibility
export { getWriteDb, getRawSql } from '../db';
