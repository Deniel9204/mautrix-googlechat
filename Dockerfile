FROM golang:1-alpine AS builder
RUN apk add --no-cache git ca-certificates build-base
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Version stamping: passed by CI (docker/build-push-action build-args); default
# to "unknown" so a plain `docker build` / `docker compose build` still works.
# .dockerignore excludes .git, so these can't be derived in-container.
ARG VERSION=unknown
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
RUN go build -tags goolm \
    -ldflags="-s -w -X main.Tag=${VERSION} -X main.Commit=${COMMIT} -X main.BuildTime=${BUILD_TIME} -X maunium.net/go/mautrix.GoModVersion=$(go list -m -f '{{.Version}}' maunium.net/go/mautrix)" \
    -o /usr/bin/mautrix-googlechat ./cmd/mautrix-googlechat

FROM alpine:3.24
RUN apk add --no-cache ffmpeg ca-certificates jq curl bash
COPY --from=builder /usr/bin/mautrix-googlechat /usr/bin/mautrix-googlechat
COPY --from=builder /build/docker-run.sh /docker-run.sh
ENV BRIDGEV2=1
VOLUME /data
WORKDIR /data
CMD ["/docker-run.sh"]
