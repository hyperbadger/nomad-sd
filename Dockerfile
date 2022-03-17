FROM golang:1.17-alpine as builder

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -v -o nomad-sd

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/nomad-sd /app/nomad-sd

WORKDIR /app
ENTRYPOINT ["/app/nomad-sd"] 
