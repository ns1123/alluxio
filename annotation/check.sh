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

PROJECT_DIR=$1

pushd ${PROJECT_DIR}/annotation

# Fetch the scripts repository if needed
if [[ -n "${PROJECT_DIR}/annotation/scripts" ]]; then
  git clone git@github.com:TachyonNexus/alluxio-scripts.git scripts
fi

# Update to the latest version
pushd "${PROJECT_DIR}/annotation/scripts"
git pull

# Install the gb tool and build and run the annotation tool.
pushd "${PROJECT_DIR}/annotation/scripts/go"
GOPATH=${PWD} go get github.com/constabulary/gb/...
./bin/gb build alluxio.org/alluxio/annotation
./bin/annotation -repo ${PROJECT_DIR} lint -fail-on-warning
