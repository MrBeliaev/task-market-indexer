FROM golang:1.22-alpine AS builder

WORKDIR /app

# Cache dependencies before copying source
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /indexer ./cmd/main.go

# ── Minimal runtime image (~5 MB) ────────────────────────────────────────────
FROM scratch

COPY --from=builder /indexer /indexer
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/indexer"]
