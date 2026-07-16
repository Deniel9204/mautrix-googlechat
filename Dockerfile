# Multi-arch (linux/amd64 + linux/arm64) via tonistiigi/xx: the builder always
# runs natively on $BUILDPLATFORM and cross-compiles to $TARGETPLATFORM with a
# clang toolchain. CGO is required (mattn/go-sqlite3 + mautrix's mxmain
# reference sqlite3.Error), so CGO_ENABLED=1 with xx's target musl-dev/gcc; this
# is far faster than QEMU-emulating the whole cgo build.
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.6.1 AS xx

FROM --platform=$BUILDPLATFORM golang:1-alpine AS builder
COPY --from=xx / /
RUN apk add --no-cache git clang lld
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
ARG TARGETPLATFORM
RUN xx-apk add --no-cache musl-dev gcc
COPY . .
# Version stamping: passed by CI (docker/build-push-action build-args); default
# to "unknown" so a plain `docker build` / `docker compose build` still works.
# .dockerignore excludes .git, so these can't be derived in-container.
ARG VERSION=unknown
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
RUN CGO_ENABLED=1 xx-go build -tags goolm \
    -ldflags="-s -w -X main.Tag=${VERSION} -X main.Commit=${COMMIT} -X main.BuildTime=${BUILD_TIME} -X maunium.net/go/mautrix.GoModVersion=$(go list -m -f '{{.Version}}' maunium.net/go/mautrix)" \
    -o /usr/bin/mautrix-googlechat ./cmd/mautrix-googlechat \
    && xx-verify /usr/bin/mautrix-googlechat

FROM alpine:3.24
RUN apk add --no-cache ffmpeg ca-certificates jq curl bash
COPY --from=builder /usr/bin/mautrix-googlechat /usr/bin/mautrix-googlechat
COPY --from=builder /build/docker-run.sh /docker-run.sh
ENV BRIDGEV2=1
VOLUME /data
WORKDIR /data
CMD ["/docker-run.sh"]
