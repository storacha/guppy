.PHONY: build test clean migration migrate-up migrate-down migrate-status migrate-reset

BINARY ?= guppy
MIGRATIONS_DIR ?= pkg/preparation/sqlrepo/migrations
DB_PATH ?= ~/.storacha/guppy/preparation.db
GOOSE := go tool goose

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
