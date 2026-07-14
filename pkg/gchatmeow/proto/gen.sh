#!/bin/sh
# Regenerates googlechat.pb.go. Requires: go install github.com/bufbuild/buf/cmd/buf@latest
#                                          go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
cd "$(dirname "$0")"
buf generate
