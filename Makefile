GO      ?= go
PROTOC  ?= protoc

.PHONY: proto build test test-race vet clean docker-build compose-up compose-down demo chaos

# Regenerate Go code from the .proto files. Requires protoc,
# protoc-gen-go and protoc-gen-go-grpc on PATH.
proto:
	$(PROTOC) --go_out=. --go_opt=module=github.com/atharv-pdarshi/kvraft \
	          --go-grpc_out=. --go-grpc_opt=module=github.com/atharv-pdarshi/kvraft \
	          proto/kv.proto proto/raft.proto

build:
	$(GO) build ./...
	$(GO) build -o bin/kvnode ./cmd/kvnode
	$(GO) build -o bin/kvctl  ./cmd/kvctl
	$(GO) build -o bin/loadgen ./cmd/loadgen

test:
	$(GO) test ./...

# The real proof: run the whole suite under the race detector.
test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

clean:
	rm -rf bin data deploy/data

docker-build:
	docker build -t kvraft:latest .

compose-up:
	cd deploy && docker compose up --build -d

compose-down:
	cd deploy && docker compose down -v

demo:
	./scripts/demo.sh

chaos:
	./scripts/chaos.sh
