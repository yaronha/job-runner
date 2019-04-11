FROM golang:1.12-stretch as build

WORKDIR /invoke
COPY . .
RUN go build ./cmd/invoke
RUN cp invoke /usr/local/bin

FROM debian:jessie-slim
COPY --from=build /usr/local/bin/invoke /usr/local/bin
ENV LOG_LEVEL info
