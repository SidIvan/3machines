FROM debian:latest

WORKDIR /app

COPY --from=build:develop /build/app ./app
#COPY --from=build:develop /build/cmd/nestor.yaml ./nestor.yaml
RUN mkdir /app/log
RUN touch /app/log/sizif.log

ENTRYPOINT ["./app", "-cfg", "sizif.yaml"]