FROM golang:latest as build

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY internal ./internal
COPY cmd/sizif ./cmd
COPY pkg ./pkg

RUN go build -o app ./cmd/main.go

FROM debian:latest as production

WORKDIR /app

COPY --from=build /build/app ./app
RUN mkdir /app/log
RUN touch /app/log/sizif.log

ENTRYPOINT ["./app", "-cfg", "sizif.yaml"]