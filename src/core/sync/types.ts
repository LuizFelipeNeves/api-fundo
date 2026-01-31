import type { FIIResponse, FIIDetails } from '../../types';
import type { DividendItem } from '../../parsers';
import type { NormalizedIndicators } from '../../parsers/indicators';
import type { ContationsTodayData } from '../../parsers/today';
import type { DocumentData } from '../../parsers/documents';
import type { DividendData } from '../../parsers/dividends';
import type { NormalizedCotations } from '../../parsers/cotations';

export interface ClockDeps {
  nowIso(): string;
  sha256(value: string): string;
}

export interface FetcherDeps {
  fetchFIIList(): Promise<FIIResponse>;
  fetchFIIDetails(code: string): Promise<{ details: FIIDetails; dividendsHistory: DividendItem[] }>;
  fetchFIIIndicators(id: string): Promise<NormalizedIndicators>;
  fetchFIICotations(id: string, days: number): Promise<NormalizedCotations>;
  fetchDividends(code: string, input?: { id?: string; dividendsHistory?: DividendItem[] }): Promise<DividendData[]>;
  fetchCotationsToday(code: string): Promise<ContationsTodayData>;
  fetchDocuments(cnpj: string): Promise<DocumentData[]>;
}

export interface RepoDeps<Db = unknown> {
  upsertFundList(db: Db, data: FIIResponse): void;
  updateFundDetails(db: Db, details: FIIDetails): void;
  getFundIdAndCnpj(db: Db, code: string): { id: string | null; cnpj: string | null } | null;
  getFundState(db: Db, code: string): {
    last_documents_max_id: number | null;
    last_historical_cotations_at: string | null;
  } | null;
  upsertIndicatorsSnapshot(db: Db, fundCode: string, fetchedAt: string, dataHash: string, data: NormalizedIndicators): boolean;
  upsertCotationsTodaySnapshot(db: Db, fundCode: string, fetchedAt: string, dataHash: string, data: ContationsTodayData): boolean;
  upsertCotationsHistoricalBrl(db: Db, fundCode: string, data: NormalizedCotations): number;
  upsertDocuments(db: Db, fundCode: string, docs: DocumentData[]): { inserted: number; maxId: number };
  updateDocumentsMaxId(db: Db, fundCode: string, maxId: number): void;
  upsertDividends(db: Db, fundCode: string, dividends: DividendData[]): number;
}
