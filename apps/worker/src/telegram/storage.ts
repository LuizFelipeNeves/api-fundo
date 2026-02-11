import { getWriteDb } from '../pipeline/db';

export type TelegramUserUpsert = {
  chatId: string;
  username?: string;
  firstName?: string;
  lastName?: string;
};

export async function upsertTelegramUser(_db: unknown, user: TelegramUserUpsert): Promise<void> {
  const sql = getWriteDb();
  await sql`
    INSERT INTO telegram_user (chat_id, username, first_name, last_name, created_at, updated_at)
    VALUES (${user.chatId}, ${user.username ?? null}, ${user.firstName ?? null}, ${user.lastName ?? null}, ${new Date()}, ${new Date()})
    ON CONFLICT (chat_id) DO UPDATE SET
      username = EXCLUDED.username,
      first_name = EXCLUDED.first_name,
      last_name = EXCLUDED.last_name,
      updated_at = EXCLUDED.updated_at
  `;
}

export async function listTelegramUserFunds(_db: unknown, chatId: string): Promise<string[]> {
  const sql = getWriteDb();
  const rows = await sql<{ fund_code: string }[]>`
    SELECT fund_code
    FROM telegram_user_fund
    WHERE chat_id = ${chatId}
    ORDER BY fund_code ASC
  `;
  return rows.map((r) => r.fund_code.toUpperCase());
}

export async function setTelegramUserFunds(_db: unknown, chatId: string, fundCodes: string[]): Promise<void> {
  const sql = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  await sql.begin(async (tx) => {
    await tx`DELETE FROM telegram_user_fund WHERE chat_id = ${chatId}`;
    if (codes.length) {
      await tx`
        INSERT INTO telegram_user_fund (chat_id, fund_code, created_at)
        VALUES ${tx(codes.map((code) => [chatId, code, new Date()]))}
        ON CONFLICT DO NOTHING
      `;
    }
  });
}

export async function addTelegramUserFunds(_db: unknown, chatId: string, fundCodes: string[]): Promise<number> {
  const sql = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return 0;
  const rows = await sql`
    INSERT INTO telegram_user_fund (chat_id, fund_code, created_at)
    VALUES ${sql(codes.map((code) => [chatId, code, new Date()]))}
    ON CONFLICT DO NOTHING
    RETURNING fund_code
  `;
  return rows.length;
}

export async function removeTelegramUserFunds(_db: unknown, chatId: string, fundCodes: string[]): Promise<number> {
  const sql = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return 0;
  const rows = await sql`
    DELETE FROM telegram_user_fund
    WHERE chat_id = ${chatId} AND fund_code = ANY(${codes})
    RETURNING fund_code
  `;
  return rows.length;
}

export async function listExistingFundCodes(_db: unknown, fundCodes: string[]): Promise<string[]> {
  const sql = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return [];
  const rows = await sql<{ code: string }[]>`
    SELECT code
    FROM fund_master
    WHERE code = ANY(${codes})
    ORDER BY code ASC
  `;
  return rows.map((r) => r.code.toUpperCase());
}

export type FundCategoryInfo = {
  code: string;
  segmento: string | null;
  sector: string | null;
  tipo_fundo: string | null;
  type: string | null;
};

export async function listFundCategoryInfoByCodes(_db: unknown, fundCodes: string[]): Promise<FundCategoryInfo[]> {
  const sql = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return [];
  const rows = await sql<FundCategoryInfo[]>`
    SELECT code, segmento, sector, tipo_fundo, type
    FROM fund_master
    WHERE code = ANY(${codes})
    ORDER BY code ASC
  `;
  return rows.map((r) => ({
    code: r.code,
    segmento: r.segmento ?? null,
    sector: r.sector ?? null,
    tipo_fundo: r.tipo_fundo ?? null,
    type: r.type ?? null,
  }));
}

export type TelegramPendingAction =
  | { kind: 'set'; codes: string[] }
  | { kind: 'remove'; codes: string[] };

export async function upsertTelegramPendingAction(_db: unknown, chatId: string, action: TelegramPendingAction): Promise<string> {
  const sql = getWriteDb();
  const now = new Date();
  await sql`
    INSERT INTO telegram_pending_action (chat_id, created_at, action_json)
    VALUES (${chatId}, ${now}, ${sql.json(action)})
    ON CONFLICT (chat_id) DO UPDATE SET
      created_at = EXCLUDED.created_at,
      action_json = EXCLUDED.action_json
  `;
  return now.toISOString();
}

export async function getTelegramPendingAction(_db: unknown, chatId: string): Promise<{ createdAt: string; action: TelegramPendingAction } | null> {
  const sql = getWriteDb();
  const rows = await sql<{ created_at: string; action_json: TelegramPendingAction }[]>`
    SELECT created_at, action_json
    FROM telegram_pending_action
    WHERE chat_id = ${chatId}
    LIMIT 1
  `;
  const row = rows[0];
  if (!row) return null;
  return { createdAt: row.created_at, action: row.action_json };
}

export async function clearTelegramPendingAction(_db: unknown, chatId: string): Promise<void> {
  const sql = getWriteDb();
  await sql`DELETE FROM telegram_pending_action WHERE chat_id = ${chatId}`;
}
