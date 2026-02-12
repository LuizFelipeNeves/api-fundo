import { createReadModelWriter } from './read-models';
import { getRawSql } from '../db';

export type ReadModelWriter = ReturnType<typeof createReadModelWriter>;

export function getReadModelWriter(): ReadModelWriter {
  return createReadModelWriter(getRawSql());
}
