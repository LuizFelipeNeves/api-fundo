package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration
type Config struct {
	DatabaseURL       string
	WorkerPoolSize    int
	SchedulerInterval time.Duration

	// PostgreSQL connection pool settings
	MaxOpenConns int
	MaxIdleConns int

	// Task intervals (in minutes)
	IntervalFundListMin       int
	IntervalFundDetailsMin    int
	IntervalCotationsMin      int
	IntervalCotationsTodayMin int
	IntervalIndicatorsMin     int
	IntervalDocumentsMin      int

	// Batch sizes
	BatchSize int

	// Timezone
	Location *time.Location

	// HTTP client settings
	CSRFToken        string
	Cookie           string
	HTTPTimeoutMS    int
	HTTPRetryMax     int
	HTTPRetryDelayMS int
}

// Load reads configuration from environment variables
func Load() (*Config, error) {
	loc, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		loc = time.UTC
	}

	return &Config{
		DatabaseURL:       getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/fii?sslmode=disable"),
		WorkerPoolSize:    getEnvInt("WORKER_POOL_SIZE", 3),
		SchedulerInterval: time.Duration(getEnvInt("SCHEDULER_INTERVAL_MS", 30000)) * time.Millisecond,

		MaxOpenConns: 5,
		MaxIdleConns: 2,

		IntervalFundListMin:       getEnvInt("INTERVAL_FUND_LIST_MIN", 30),
		IntervalFundDetailsMin:    getEnvInt("INTERVAL_FUND_DETAILS_MIN", 15),
		IntervalCotationsMin:      getEnvInt("INTERVAL_COTATIONS_MIN", 5),
		IntervalCotationsTodayMin: getEnvInt("INTERVAL_COTATIONS_TODAY_MIN", 5),
		IntervalIndicatorsMin:     getEnvInt("INTERVAL_INDICATORS_MIN", 30),
		IntervalDocumentsMin:      getEnvInt("INTERVAL_DOCUMENTS_MIN", 25),

		BatchSize: getEnvInt("BATCH_SIZE", 3),

		Location: loc,

		CSRFToken:        getEnv("CSRF_TOKEN", "CTGmgCUHY62gqvsBGnHJRUWtuRZhmLw5WXQNPjBn"),
		Cookie:           getEnv("COOKIE", "XSRF-TOKEN=eyJpdiI6InNSa25kbjBBai9Oc0xKUFlrU1NwN0E9PSIsInZhbHVlIjoiNnc0SElMV2dUSzRTNjVBNktrcW1iRHBNRUp4cFhyKzNQUnZqSnR4RnVGMEwyWVJPalZOVHFSczRMVHJvWFl5MTJBWGYyZjVwbjM2MFhyS00ybnIxUlp3TTF4Nzh0dFp2NGdaTVN0K1BWMzhrRzVmdXJZajBvRkNmTmo4UXdndjkiLCJtYWMiOiJjODJmNDcwOGI1YTA4NDdkMDYwOTE2OWRiNGIyNTUwNDU5MGJlNTczZDNjMGNmZDMzN2EzZjFlZmYxYzViNDM0IiwidGFnIjoiIn0%3D; laravel_session=eyJpdiI6IklGTzFWazBJN3RjSVIrQnF1by9adHc9PSIsInZhbHVlIjoieWlidUp0a0psUGVhQ0hwUXRRM1NTYkZ1ZldQc2dETjlwZFl0aHJ5RENLNXBVQndqYkpjaytrWFhwTDlIamFwMTNRVjRpbnhiUS9UV0FFR3BjVS9malljTFZLMmp2dHV0NStqck1xbnFFOTFzZ1VlenpZc2RScWtaMHd4RExzUHMiLCJtYWMiOiJmODk1YmNiZGM3MWNlNzY4ZTllODY2YTFlY2I0MDMwMzE1ZTk1NjUwYjc1NTdlYTA2ZmE5Nzk4ZDM2MzE0YjZhIiwidGFnIjoiIn0%3D"),
		HTTPTimeoutMS:    getEnvInt("HTTP_TIMEOUT_MS", 25000),
		HTTPRetryMax:     getEnvInt("HTTP_RETRY_MAX", 5),
		HTTPRetryDelayMS: getEnvInt("HTTP_RETRY_DELAY_MS", 2000),
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
