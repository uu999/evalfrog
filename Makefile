.PHONY: build test test-race vet fmt-check check dev down

build:
	go build ./cmd/...

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l cmd internal tests)" || (gofmt -l cmd internal tests && exit 1)

check: fmt-check vet test test-race build

dev:
	./scripts/dev.sh

down:
	./scripts/down.sh
