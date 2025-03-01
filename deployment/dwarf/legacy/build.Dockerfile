FROM golang:latest

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY internal ./internal
COPY cmd/dwarf ./cmd
COPY pkg ./pkg

RUN go build -o app ./cmd/main.go

