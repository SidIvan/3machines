
FROM debian:latest

WORKDIR /app

COPY --from=build:develop /build/app ./app
COPY --from=build:develop /build/cmd/test.yaml ./test.yaml
RUN mkdir /app/log
RUN touch /app/log/writer.log

ENTRYPOINT ["./app", "-cfg", "./writer2.yaml"]