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
		SchedulerInterval: time.Duration(getEnvInt("SCHEDULER_INTERVAL_MS", 60000)) * time.Millisecond,

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
