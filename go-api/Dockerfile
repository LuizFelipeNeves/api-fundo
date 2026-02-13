# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
    -trimpath \
    -ldflags="-w -s" \
    -o /out/api \
    ./cmd/api

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/api /api

ENV TZ=America/Sao_Paulo

USER nonroot:nonroot

ENTRYPOINT ["/api"]
