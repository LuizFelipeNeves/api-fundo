import { Context } from 'hono';

type HandlerResult<T> = { data: T };

export function createHandler<T>(
  handler: (c: Context) => Promise<HandlerResult<T> | Response>,
  errorMessage: string
) {
  return async (c: Context) => {
    try {
      const result = await handler(c);
      if (result instanceof Response) return result;
      return c.json(result.data);
    } catch (error) {
      if (error instanceof Error && error.message === 'FII_NOT_FOUND') {
        return c.json({
          error: 'FII não encontrado',
          message: 'O fundo de investimento informado não existe',
        }, 404);
      }
      console.error(`${errorMessage}:`, error);
      return c.json({ error: errorMessage }, 500);
    }
  };
}
