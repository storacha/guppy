.PHONY: build test clean

BINARY ?= guppy

build:
	go build -o $(BINARY) .

test:
	go test ./...

clean:
	go clean ./...
	rm -f $(BINARY)
