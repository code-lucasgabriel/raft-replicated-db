.PHONY: proto build run tidy clean

# Generate Go code from .proto files. Requires `buf` (https://buf.build).
proto:
	buf generate

build:
	go build -o bin/node ./cmd/main

run: build
	./bin/node

tidy:
	go mod tidy

clean:
	rm -rf bin internal/pb
