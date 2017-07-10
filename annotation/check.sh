#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

set -e

SCRIPTS_DIR="$(cd "$(dirname "$0" )"; pwd)/scripts"

# Fetch the scripts repository if needed
if [[ ! -e "${SCRIPTS_DIR}" ]]; then
  git clone git@github.com:TachyonNexus/alluxio-scripts.git "${SCRIPTS_DIR}"
fi

# Update to the latest version
cd "${SCRIPTS_DIR}"
git pull

# Install the gb tool and build and run the annotation tool.
# Before installing gb using "go get", we need to unset GOBIN
# so that gb is installed as annotation/scripts/go/bin/gb
# instead of ${GOBIN}/gb.
unset GOBIN
cd "${SCRIPTS_DIR}/go"
GOPATH="${PWD}" go get github.com/constabulary/gb/...
./bin/gb build alluxio.org/alluxio/annotation
./bin/annotation -repo "${SCRIPTS_DIR}/../.." lint -fail-on-warning
