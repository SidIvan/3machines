FROM debian:latest

WORKDIR /app

COPY --from=build:develop /build/app ./app
COPY --from=build:develop /build/cmd/test.yaml ./test.yaml
COPY your_certificate.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

ENTRYPOINT ["./app", "-cfg", "./test.yaml"]