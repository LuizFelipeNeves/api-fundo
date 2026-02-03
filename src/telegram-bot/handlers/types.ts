import { getDb } from '../../db';
import { createTelegramService } from '../telegram-api';

type TelegramService = ReturnType<typeof createTelegramService>;

export type HandlerDeps = {
  db: ReturnType<typeof getDb>;
  telegram: TelegramService;
  chatIdStr: string;
};
