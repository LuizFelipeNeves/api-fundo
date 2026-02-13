# Build stage
FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src/go-worker

# Copy go mod files
COPY go-worker/go.mod go-worker/go.sum ./
RUN go mod download

# Copy source code
COPY go-worker/. .

# Build static binary
RUN mkdir -p /out && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /out/worker \
    ./cmd/worker

# Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot

# Copy timezone data
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy CA certificates
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary
COPY --from=builder /out/worker /worker

# Copy database schema for runtime auto-apply
COPY database/schema.sql /app/database/schema.sql

# Set timezone
ENV TZ=America/Sao_Paulo

# Run as non-root
USER nonroot:nonroot

ENTRYPOINT ["/worker"]
