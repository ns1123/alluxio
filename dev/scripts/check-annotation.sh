#!/bin/bash

SCRIPTS_DIR="$(cd "$(dirname "$0" )"; pwd)"
REPO_ROOT=$SCRIPTS_DIR/../..
GOPATH=$SCRIPTS_DIR go run $SCRIPTS_DIR/src/alluxio.com/annotation/main.go -repo $REPO_ROOT lint -fail-on-warning

