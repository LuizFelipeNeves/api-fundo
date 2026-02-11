import type { createTelegramService } from '../../telegram-bot/telegram-api';

type TelegramService = ReturnType<typeof createTelegramService>;

export type HandlerDeps = {
  db: unknown;
  telegram: TelegramService;
  chatIdStr: string;
};
