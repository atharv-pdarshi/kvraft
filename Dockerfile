# Multi-stage build: compile with the Go toolchain, ship a small runtime image.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/kvnode ./cmd/kvnode \
 && CGO_ENABLED=0 go build -trimpath -o /out/kvctl  ./cmd/kvctl

FROM alpine:3.20
COPY --from=build /out/kvnode /usr/local/bin/kvnode
COPY --from=build /out/kvctl  /usr/local/bin/kvctl

# Each node serves both the Raft and KV gRPC services on this port.
EXPOSE 3000
ENTRYPOINT ["kvnode"]
