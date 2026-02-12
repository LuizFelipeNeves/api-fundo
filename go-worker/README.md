# Go Worker - Ultra Simple, Ultra Low RAM

Ultra-simple Go worker for fund data collection. Eliminates RabbitMQ by using the existing `fund_state` table as the sole scheduling mechanism.

## Architecture

- **No queues**: Direct timestamp-based scheduling from PostgreSQL
- **Synchronous pipeline**: `collect → persist` in one flow
- **Low memory**: Targets 20-40MB RAM usage
- **Small worker pool**: 2-4 goroutines maximum
- **Minimal dependencies**: Only `lib/pq` for PostgreSQL

## Building

```bash
# Build locally
go mod tidy
go build -o worker ./cmd/worker

# Build Docker image
docker build -t go-worker .
```

## Running

### Local Development

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/fii?sslmode=disable"
export WORKER_POOL_SIZE=3
export SCHEDULER_INTERVAL_MS=60000

./worker
```

### Docker Compose

```bash
docker-compose up go-worker
```

## Configuration

All configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/fii?sslmode=disable` | PostgreSQL connection string |
| `WORKER_POOL_SIZE` | `3` | Number of worker goroutines |
| `SCHEDULER_INTERVAL_MS` | `60000` | Scheduler tick interval (ms) |
| `BATCH_SIZE` | `3` | Max funds per batch |
| `INTERVAL_FUND_LIST_MIN` | `30` | Fund list update interval (minutes) |
| `INTERVAL_FUND_DETAILS_MIN` | `15` | Fund details update interval (minutes) |
| `INTERVAL_COTATIONS_MIN` | `5` | Historical cotations interval (minutes) |
| `INTERVAL_COTATIONS_TODAY_MIN` | `5` | Today's cotations interval (minutes) |
| `INTERVAL_INDICATORS_MIN` | `30` | Indicators update interval (minutes) |
| `INTERVAL_DOCUMENTS_MIN` | `25` | Documents sync interval (minutes) |

## Memory Optimization

- Fixed small worker pool (no goroutine-per-job)
- Small batches (1-5 funds max)
- Strict DB connection limits (5 max open, 2 idle)
- No in-memory caching
- Direct JSON decoding into structs
- Small channel buffers

## Business Hours

Certain tasks only run during São Paulo business hours (10:00-18:30, weekdays):
- Fund list updates
- Fund details
- Today's cotations
- Indicators

Other tasks run anytime:
- Historical cotations backfill
- Documents sync

EOD cotation runs once per day after 18:30 using PostgreSQL advisory locks.
