FROM golang:1.21-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -o /pipeline-daemon ./cmd/daemon

FROM alpine:3.19

RUN apk add --no-cache sqlite-libs ca-certificates

WORKDIR /app
COPY --from=builder /pipeline-daemon /app/pipeline-daemon

EXPOSE 8080

ENTRYPOINT ["/app/pipeline-daemon"]
