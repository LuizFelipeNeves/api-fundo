FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src/go-worker

COPY go-worker/go.mod go-worker/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY go-worker/. .

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -trimpath \
    -ldflags="-w -s" \
    -o /out/worker \
    ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/worker /worker
COPY database/schema.sql /app/database/schema.sql

ENV TZ=America/Sao_Paulo

USER nonroot:nonroot

ENTRYPOINT ["/worker"]
