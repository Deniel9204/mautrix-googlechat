FROM golang:1-alpine AS builder
RUN apk add --no-cache git ca-certificates build-base
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -tags goolm -o /usr/bin/mautrix-googlechat ./cmd/mautrix-googlechat

FROM alpine:3.24
RUN apk add --no-cache ffmpeg ca-certificates jq curl bash
COPY --from=builder /usr/bin/mautrix-googlechat /usr/bin/mautrix-googlechat
COPY --from=builder /build/docker-run.sh /docker-run.sh
ENV BRIDGEV2=1
VOLUME /data
WORKDIR /data
CMD ["/docker-run.sh"]
