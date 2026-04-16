.PHONY: build test clean migration migrate-up migrate-down migrate-status migrate-reset release guppy-prod guppy-debug docker-setup docker-prod docker-dev

BINARY ?= guppy
MIGRATIONS_DIR ?= pkg/preparation/sqlrepo/migrations
DB_PATH ?= ~/.storacha/guppy/preparation.db
GOOSE := go tool goose

VERSION=$(shell awk -F'"' '/"version":/ {print $$4}' version.json)
COMMIT=$(shell git rev-parse --short HEAD)
DATE=$(shell date -u -Iseconds)
GOFLAGS=-ldflags="-X github.com/storacha/guppy/pkg/build.version=$(VERSION) -X github.com/storacha/guppy/pkg/build.Commit=$(COMMIT) -X github.com/storacha/guppy/pkg/build.Date=$(DATE) -X github.com/storacha/guppy/pkg/build.BuiltBy=make"
DOCKER?=$(shell which docker)

build:
	go build -o $(BINARY) .

test:
	go test ./...

clean:
	go clean ./...
	rm -f $(BINARY)

migration:
	@test -n "$(NAME)" || (echo "Error: NAME is required. Usage: make migration NAME=your_migration_name" && exit 1)
	$(GOOSE) -dir $(MIGRATIONS_DIR) create $(NAME) sql

migrate-up:
	$(GOOSE) -dir $(MIGRATIONS_DIR) sqlite3 $(DB_PATH) up

migrate-down:
	$(GOOSE) -dir $(MIGRATIONS_DIR) sqlite3 $(DB_PATH) down

migrate-status:
	$(GOOSE) -dir $(MIGRATIONS_DIR) sqlite3 $(DB_PATH) status

migrate-reset:
	$(GOOSE) -dir $(MIGRATIONS_DIR) sqlite3 $(DB_PATH) reset

release:
	./scripts/release.sh

# Production binary - stripped symbols for smaller size
guppy-prod:
	@echo "Building guppy (production)..."
	go build -ldflags="-s -w -X github.com/storacha/guppy/pkg/build.version=$(VERSION) -X github.com/storacha/guppy/pkg/build.Commit=$(COMMIT) -X github.com/storacha/guppy/pkg/build.Date=$(DATE) -X github.com/storacha/guppy/pkg/build.BuiltBy=make" -o ./guppy .

# Debug binary - no optimizations, no inlining, full symbols
guppy-debug:
	@echo "Building guppy (debug)..."
	go build -gcflags="all=-N -l" -ldflags="-X github.com/storacha/guppy/pkg/build.version=$(VERSION) -X github.com/storacha/guppy/pkg/build.Commit=$(COMMIT) -X github.com/storacha/guppy/pkg/build.Date=$(DATE) -X github.com/storacha/guppy/pkg/build.BuiltBy=make-debug" -o ./guppy .

# Docker targets (multi-arch: amd64 + arm64)
docker-setup:
	$(DOCKER) buildx create --name multiarch --use 2>/dev/null || $(DOCKER) buildx use multiarch

docker-prod: docker-setup
	$(DOCKER) buildx build --platform linux/amd64,linux/arm64 --target prod \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg COMMIT=$(COMMIT) \
	  --build-arg DATE=$(DATE) \
	  --build-arg BUILT_BY=make \
	  -t guppy:latest .

docker-dev: docker-setup
	$(DOCKER) buildx build --platform linux/amd64,linux/arm64 \
	  -f Dockerfile.dev \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg COMMIT=$(COMMIT) \
	  --build-arg DATE=$(DATE) \
	  --build-arg BUILT_BY=make-debug \
	  -t guppy:dev .
