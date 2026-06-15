BINARY := indexer
IMAGE  := taskmarket-indexer

GCI_SECTIONS := -s standard -s default -s "Prefix(github.com/taskmarket)"

.PHONY: build run test test-cover lint fmt tidy docker-build clean

build:
	go build -o bin/$(BINARY) ./cmd/main.go

run:
	set -a && [ -f .env ] && . ./.env; set +a; go run ./cmd/main.go

test:
	go test ./...

test-cover:
	go test -cover ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .
	gci write --skip-generated $(GCI_SECTIONS) . cmd internal

tidy:
	go mod tidy

docker-build:
	docker build -t $(IMAGE) .

clean:
	rm -rf bin
