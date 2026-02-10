import type { FIIResponse, FIIDetails } from '../../types';
import type { DividendItem } from '../../parsers';
import type { NormalizedIndicators } from '../../parsers/indicators';
import type { CotationsTodayData } from '../../parsers/today';
import type { DocumentData } from '../../parsers/documents';
import type { DividendData } from '../../parsers/dividends';
import type { NormalizedCotations } from '../../parsers/cotations';
import type { FnetSessionData } from '../../db';

export interface ClockDeps {
  nowIso(): string;
  sha256(value: string): string;
}

export type FnetSessionCallbacks = {
  getSession: () => FnetSessionData;
  saveSession: (cnpj: string, jsessionId: string, lastValidAt: number) => void;
};

export interface FetcherDeps {
  fetchFIIList(): Promise<FIIResponse>;
  fetchFIIDetails(code: string): Promise<{ details: FIIDetails; dividendsHistory: DividendItem[] }>;
  fetchFIIIndicators(id: string): Promise<NormalizedIndicators>;
  fetchFIICotations(id: string, days: number): Promise<NormalizedCotations>;
  fetchDividends(code: string, input?: { id?: string; dividendsHistory?: DividendItem[] }): Promise<DividendData[]>;
  fetchCotationsToday(code: string): Promise<CotationsTodayData>;
  fetchDocuments(cnpj: string, callbacks: FnetSessionCallbacks): Promise<DocumentData[]>;
}

export interface RepoDeps<Db = unknown> {
  upsertFundList(db: Db, data: FIIResponse): void;
  updateFundDetails(db: Db, details: FIIDetails): void;
  getFundIdAndCnpj(db: Db, code: string): { id: string | null; cnpj: string | null } | null;
  getFundState(db: Db, code: string): {
    last_documents_max_id: number | null;
    last_historical_cotations_at: string | null;
  } | null;
  getDividendCount(db: Db, fundCode: string): number;
  upsertIndicatorsSnapshot(db: Db, fundCode: string, fetchedAt: string, dataHash: string, data: NormalizedIndicators): boolean;
  upsertCotationsTodaySnapshot(db: Db, fundCode: string, fetchedAt: string, dataHash: string, data: CotationsTodayData): boolean;
  upsertCotationsHistoricalBrl(db: Db, fundCode: string, data: NormalizedCotations): number;
  upsertDocuments(db: Db, fundCode: string, docs: DocumentData[]): { inserted: number; maxId: number };
  updateDocumentsMaxId(db: Db, fundCode: string, maxId: number): void;
  upsertDividends(db: Db, fundCode: string, dividends: DividendData[]): number;
  getFnetSession(db: Db, cnpj: string): FnetSessionData;
  saveFnetSession(db: Db, cnpj: string, jsessionId: string, lastValidAt: number): void;
}
