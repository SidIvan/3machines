FROM debian:latest

WORKDIR /app

COPY --from=build:develop /build/app ./app
#COPY --from=build:develop /build/cmd/nestor.yaml ./nestor.yaml
RUN mkdir /app/log
RUN touch /app/log/dwarf.log

ENTRYPOINT ["./app", "-cfg", "./dwarf.yaml"]