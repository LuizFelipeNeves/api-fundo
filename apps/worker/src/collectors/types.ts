export type CollectRequest = {
  collector: string;
  fund_code?: string;
  cnpj?: string;
  range?: { from?: string; to?: string; days?: number };
  correlation_id?: string;
  triggered_by?: string;
  priority?: number;
  meta?: Record<string, unknown>;
};

export type CollectResult = {
  collector: string;
  fetched_at: string;
  payload: unknown;
  meta?: Record<string, unknown>;
};

export type CollectorContext = {
  http: {
    getJson<T>(url: string, opts?: { headers?: Record<string, string>; timeoutMs?: number }): Promise<T>;
    getText(url: string, opts?: { headers?: Record<string, string>; timeoutMs?: number }): Promise<string>;
    postForm<T>(url: string, body: string, opts?: { headers?: Record<string, string>; timeoutMs?: number }): Promise<T>;
  };
  publish?(queue: string, body: Buffer): void;
};

export type Collector = {
  name: string;
  supports(request: CollectRequest): boolean;
  collect(request: CollectRequest, ctx: CollectorContext): Promise<CollectResult | void>;
};
