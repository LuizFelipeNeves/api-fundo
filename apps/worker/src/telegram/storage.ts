import { getWriteDb } from '../db';
import { telegramUser, telegramUserFund, telegramPendingAction, fundMaster } from '../db/schema';
import { eq, and, inArray, asc } from 'drizzle-orm';

export type TelegramUserUpsert = {
  chatId: string;
  username?: string;
  firstName?: string;
  lastName?: string;
};

export async function upsertTelegramUser(_db: unknown, user: TelegramUserUpsert): Promise<void> {
  const db = getWriteDb();
  await db.insert(telegramUser)
    .values({
      chatId: user.chatId,
      telegramId: user.chatId,
      username: user.username ?? null,
      firstName: user.firstName ?? null,
      lastName: user.lastName ?? null,
    })
    .onConflictDoUpdate({
      target: telegramUser.chatId,
      set: {
        username: user.username ?? null,
        firstName: user.firstName ?? null,
        lastName: user.lastName ?? null,
      },
    });
}

export async function listTelegramUserFunds(_db: unknown, chatId: string): Promise<string[]> {
  const db = getWriteDb();
  const rows = await db.select({ fundCode: telegramUserFund.fundCode })
    .from(telegramUserFund)
    .innerJoin(telegramUser, eq(telegramUserFund.userId, telegramUser.id))
    .where(eq(telegramUser.chatId, chatId))
    .orderBy(asc(telegramUserFund.fundCode));
  return rows.map((r) => r.fundCode?.toUpperCase() ?? '');
}

export async function setTelegramUserFunds(_db: unknown, chatId: string, fundCodes: string[]): Promise<void> {
  const db = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);

  await db.transaction(async (tx) => {
    const userRows = await tx.select({ id: telegramUser.id })
      .from(telegramUser)
      .where(eq(telegramUser.chatId, chatId));
    const user = userRows[0];

    if (user) {
      await tx.delete(telegramUserFund)
        .where(eq(telegramUserFund.userId, user.id));
    }

    if (codes.length && user) {
      const values = codes.map((code) => ({
        userId: user.id,
        fundCode: code,
      }));
      await tx.insert(telegramUserFund).values(values).onConflictDoNothing();
    }
  });
}

export async function addTelegramUserFunds(_db: unknown, chatId: string, fundCodes: string[]): Promise<number> {
  const db = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return 0;

  let count = 0;
  await db.transaction(async (tx) => {
    const userRows = await tx.select({ id: telegramUser.id })
      .from(telegramUser)
      .where(eq(telegramUser.chatId, chatId));
    const user = userRows[0];

    if (user) {
      for (const code of codes) {
        const result = await tx.insert(telegramUserFund)
          .values({ userId: user.id, fundCode: code })
          .onConflictDoNothing()
          .returning();
        if (result.length > 0) count++;
      }
    }
  });
  return count;
}

export async function removeTelegramUserFunds(_db: unknown, chatId: string, fundCodes: string[]): Promise<number> {
  const db = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return 0;

  let count = 0;
  await db.transaction(async (tx) => {
    const userRows = await tx.select({ id: telegramUser.id })
      .from(telegramUser)
      .where(eq(telegramUser.chatId, chatId));
    const user = userRows[0];

    if (user) {
      const result = await tx.delete(telegramUserFund)
        .where(and(
          eq(telegramUserFund.userId, user.id),
          inArray(telegramUserFund.fundCode, codes)
        ))
        .returning();
      count = result.length;
    }
  });
  return count;
}

export async function listExistingFundCodes(_db: unknown, fundCodes: string[]): Promise<string[]> {
  const db = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return [];

  const rows = await db.select({ code: fundMaster.code })
    .from(fundMaster)
    .where(inArray(fundMaster.code, codes))
    .orderBy(asc(fundMaster.code));
  return rows.map((r) => r.code ?? '');
}

export type FundCategoryInfo = {
  code: string;
  segmento: string | null;
  sector: string | null;
  tipo_fundo: string | null;
  type: string | null;
};

export async function listFundCategoryInfoByCodes(_db: unknown, fundCodes: string[]): Promise<FundCategoryInfo[]> {
  const db = getWriteDb();
  const codes = Array.from(new Set(fundCodes.map((c) => c.toUpperCase()))).filter(Boolean);
  if (!codes.length) return [];

  const rows = await db.select({
    code: fundMaster.code,
    segmento: fundMaster.segmento,
    sector: fundMaster.sector,
    tipo_fundo: fundMaster.tipoFundo,
    type: fundMaster.type,
  })
    .from(fundMaster)
    .where(inArray(fundMaster.code, codes))
    .orderBy(asc(fundMaster.code));
  return rows.map((r) => ({
    code: r.code ?? '',
    segmento: r.segmento,
    sector: r.sector,
    tipo_fundo: r.tipo_fundo,
    type: r.type,
  }));
}

export type TelegramPendingAction =
  | { kind: 'set'; codes: string[] }
  | { kind: 'remove'; codes: string[] };

export async function upsertTelegramPendingAction(_db: unknown, chatId: string, action: TelegramPendingAction): Promise<string> {
  const db = getWriteDb();
  const now = new Date();
  await db.insert(telegramPendingAction)
    .values({
      chatId,
      actionType: action.kind,
      payload: action as any,
      createdAt: now,
    })
    .onConflictDoUpdate({
      target: telegramPendingAction.chatId,
      set: {
        actionType: action.kind,
        payload: action as any,
        createdAt: now,
      },
    });
  return now.toISOString();
}

export async function getTelegramPendingAction(_db: unknown, chatId: string): Promise<{ createdAt: string; action: TelegramPendingAction } | null> {
  const db = getWriteDb();
  const rows = await db.select({
    createdAt: telegramPendingAction.createdAt,
    actionType: telegramPendingAction.actionType,
    payload: telegramPendingAction.payload,
  })
    .from(telegramPendingAction)
    .where(eq(telegramPendingAction.chatId, chatId));
  const row = rows[0];

  if (!row) return null;

  return {
    createdAt: row.createdAt?.toISOString() ?? '',
    action: { kind: row.actionType as 'set' | 'remove', ...(row.payload as any) },
  };
}

export async function clearTelegramPendingAction(_db: unknown, chatId: string): Promise<void> {
  const db = getWriteDb();
  await db.delete(telegramPendingAction)
    .where(eq(telegramPendingAction.chatId, chatId));
}
