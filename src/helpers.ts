import { Context } from 'hono';

type HandlerResult<T> = { data: T };

export function createHandler<T>(
  handler: (c: Context) => Promise<HandlerResult<T>>,
  errorMessage: string
) {
  return async (c: Context) => {
    try {
      const result = await handler(c);
      return c.json(result.data);
    } catch (error) {
      console.error(`${errorMessage}:`, error);
      return c.json({ error: errorMessage }, 500);
    }
  };
}
