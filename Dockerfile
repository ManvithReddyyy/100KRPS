FROM golang:1.23-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/safepath-api ./cmd/api

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /bin/safepath-api /app/safepath-api

RUN adduser -D -u 10001 appuser
USER appuser

EXPOSE 8081 6060

ENV PORT=8081
ENTRYPOINT ["/app/safepath-api"]
