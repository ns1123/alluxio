#!/usr/bin/env bash

set -e

SCRIPTS_DIR="$(cd "$(dirname "$0" )"; pwd)"
REPO_ROOT=$SCRIPTS_DIR/../..
GOPATH=$SCRIPTS_DIR/vendor; go run $GOPATH/src/github.com/TachyonNexus/golluxio/annotation/main.go -repo $REPO_ROOT lint -fail-on-warning

