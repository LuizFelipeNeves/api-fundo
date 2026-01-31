import type Database from 'better-sqlite3';
import { nowIso } from '../db';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { and, asc, eq, inArray } from 'drizzle-orm';
import { fundMaster, telegramPendingAction, telegramUser, telegramUserFund } from '../db/schema';

export type TelegramUserUpsert = {
  chatId: string;
  username?: string;
  firstName?: string;
  lastName?: string;
};

export function upsertTelegramUser(db: Database.Database, user: TelegramUserUpsert): void {
  const now = nowIso();
  const orm = drizzle(db);
  orm
    .insert(telegramUser)
    .values({
      chat_id: user.chatId,
      username: user.username ?? null,
      first_name: user.firstName ?? null,
      last_name: user.lastName ?? null,
      created_at: now,
      updated_at: now,
    })
    .onConflictDoUpdate({
      target: telegramUser.chat_id,
      set: {
        username: user.username ?? null,
        first_name: user.firstName ?? null,
        last_name: user.lastName ?? null,
        updated_at: now,
      },
    })
    .run();
}

export function listTelegramUserFunds(db: Database.Database, chatId: string): string[] {
  const orm = drizzle(db);
  const rows = orm
    .select({ code: telegramUserFund.fund_code })
    .from(telegramUserFund)
    .where(eq(telegramUserFund.chat_id, chatId))
    .orderBy(asc(telegramUserFund.fund_code))
    .all();
  return rows.map((r) => r.code.toUpperCase());
}

export function setTelegramUserFunds(db: Database.Database, chatId: string, fundCodes: string[]): void {
  const now = nowIso();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  const orm = drizzle(db);
  orm.transaction((tx) => {
    tx.delete(telegramUserFund).where(eq(telegramUserFund.chat_id, chatId)).run();
    for (const code of codes) {
      tx.insert(telegramUserFund)
        .values({ chat_id: chatId, fund_code: code, created_at: now })
        .onConflictDoNothing()
        .run();
    }
  });
}

export function addTelegramUserFunds(db: Database.Database, chatId: string, fundCodes: string[]): number {
  const now = nowIso();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  const orm = drizzle(db);
  let added = 0;
  orm.transaction((tx) => {
    for (const code of codes) {
      added += tx
        .insert(telegramUserFund)
        .values({ chat_id: chatId, fund_code: code, created_at: now })
        .onConflictDoNothing()
        .run().changes;
    }
  });
  return added;
}

export function removeTelegramUserFunds(db: Database.Database, chatId: string, fundCodes: string[]): number {
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  const orm = drizzle(db);
  let removed = 0;
  orm.transaction((tx) => {
    for (const code of codes) {
      removed += tx
        .delete(telegramUserFund)
        .where(and(eq(telegramUserFund.chat_id, chatId), eq(telegramUserFund.fund_code, code)))
        .run().changes;
    }
  });
  return removed;
}

export function listExistingFundCodes(db: Database.Database, fundCodes: string[]): string[] {
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (codes.length === 0) return [];
  const orm = drizzle(db);
  const rows = orm
    .select({ code: fundMaster.code })
    .from(fundMaster)
    .where(inArray(fundMaster.code, codes))
    .orderBy(asc(fundMaster.code))
    .all();
  return rows.map((r) => r.code.toUpperCase());
}

export function listTelegramChatIdsByFundCode(db: Database.Database, fundCode: string): string[] {
  const orm = drizzle(db);
  const rows = orm
    .select({ chatId: telegramUserFund.chat_id })
    .from(telegramUserFund)
    .where(eq(telegramUserFund.fund_code, fundCode.toUpperCase()))
    .orderBy(asc(telegramUserFund.chat_id))
    .all();
  return rows.map((r) => r.chatId);
}

export type FundCategoryInfo = {
  code: string;
  segmento: string | null;
  sector: string | null;
  tipo_fundo: string | null;
  type: string | null;
};

export function listFundCategoryInfoByCodes(db: Database.Database, fundCodes: string[]): FundCategoryInfo[] {
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (codes.length === 0) return [];
  const orm = drizzle(db);
  return orm
    .select({
      code: fundMaster.code,
      segmento: fundMaster.segmento,
      sector: fundMaster.sector,
      tipo_fundo: fundMaster.tipo_fundo,
      type: fundMaster.type,
    })
    .from(fundMaster)
    .where(inArray(fundMaster.code, codes))
    .orderBy(asc(fundMaster.code))
    .all();
}

export type TelegramPendingAction =
  | { kind: 'set'; codes: string[] }
  | { kind: 'remove'; codes: string[] };

export function upsertTelegramPendingAction(db: Database.Database, chatId: string, action: TelegramPendingAction): void {
  const now = nowIso();
  const orm = drizzle(db);
  orm
    .insert(telegramPendingAction)
    .values({
      chat_id: chatId,
      created_at: now,
      action_json: JSON.stringify(action),
    })
    .onConflictDoUpdate({
      target: telegramPendingAction.chat_id,
      set: {
        created_at: now,
        action_json: JSON.stringify(action),
      },
    })
    .run();
}

export function getTelegramPendingAction(db: Database.Database, chatId: string): { createdAt: string; action: TelegramPendingAction } | null {
  const orm = drizzle(db);
  const row = orm
    .select({ createdAt: telegramPendingAction.created_at, json: telegramPendingAction.action_json })
    .from(telegramPendingAction)
    .where(eq(telegramPendingAction.chat_id, chatId))
    .get();
  if (!row?.json) return null;
  try {
    return { createdAt: row.createdAt, action: JSON.parse(row.json) as TelegramPendingAction };
  } catch {
    return null;
  }
}

export function clearTelegramPendingAction(db: Database.Database, chatId: string): void {
  const orm = drizzle(db);
  orm.delete(telegramPendingAction).where(eq(telegramPendingAction.chat_id, chatId)).run();
}
