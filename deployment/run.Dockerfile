
FROM debian:latest

WORKDIR /app

COPY --from=build:develop /build/app ./app
COPY --from=build:develop /build/cmd/writer.yaml ./writer.yaml
RUN mkdir /app/log
RUN touch /app/log/writer.log

ENTRYPOINT ["./app", "-cfg", "./writer.yaml"]