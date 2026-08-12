.PHONY: build test test-contract test-fuzz test-race vet fmt-check check dev down

build:
	go build ./cmd/...

test:
	go test ./...

test-contract:
	go test -count=20 ./contracts/ir ./contracts/dsl ./contracts/source-map ./contracts/openapi ./internal/ir ./internal/catalog ./internal/dsl ./internal/sourcemap ./internal/compiler ./internal/access ./internal/resources ./internal/adapters/httpapi ./internal/runtime ./internal/runtime/engine

test-fuzz:
	go test ./internal/ir -run='^$$' -fuzz=FuzzParser -fuzztime=5s
	go test ./internal/ir -run='^$$' -fuzz=FuzzLogicalID -fuzztime=5s

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l cmd contracts internal tests)" || (gofmt -l cmd contracts internal tests && exit 1)

check: fmt-check vet test test-contract test-race build

dev:
	./scripts/dev.sh

down:
	./scripts/down.sh
