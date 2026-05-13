FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/main

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/node /node
EXPOSE 5000
ENTRYPOINT ["/node"]
