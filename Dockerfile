# Stage 1: Build static Go binary
FROM golang:1.27-alpine AS builder

WORKDIR /app

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /vocalflow \
    ./cmd/server

# Stage 2: Minimal runtime image
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /vocalflow /vocalflow

EXPOSE 8085

ENV PORT=8085

ENTRYPOINT ["/vocalflow"]
