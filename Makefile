.PHONY: proto build run tidy clean

# Generate Go code from .proto files. Requires `buf` (https://buf.build).
proto:
	buf generate

build:
	go build -o bin/node ./cmd/main
	go build -o bin/client ./cmd/client

run: build
	./bin/node

test:
	go test -race ./...

tidy:
	go mod tidy

clean:
	rm -rf bin internal/pb
