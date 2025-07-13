FROM golang:1.24

WORKDIR /root
COPY ../../src ./src
COPY ../../go.sum .
COPY ../../go.mod .

ENV APP_ENV=dev

RUN go mod tidy
RUN go mod download

RUN go build -o /bin/migration_worker ./src


FROM debian:bookworm-slim

COPY --from=0 /bin/migration_worker /bin/migration_worker

CMD ["/bin/migration_worker"]