FROM golang:latest as build

WORKDIR /build

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY internal ./internal
COPY cmd/nestor ./cmd
COPY pkg ./pkg

RUN go build -o app ./cmd/main.go

FROM debian:latest as production

WORKDIR /app

COPY --from=build /build/app ./app
RUN mkdir /app/log
RUN touch /app/log/nestor.log

COPY deployment/nestor/etc /etc
RUN apt-get update && apt-get install -y cron logrotate
RUN chmod 0644 /etc/cron.d/logrotate

COPY deployment/nestor/entrypoint.sh /
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]