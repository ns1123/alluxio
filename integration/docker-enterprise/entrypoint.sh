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

if [[ $# -ne 1 ]]; then
  echo 'expected one argument: "master", "worker", or "proxy"'
  exit 1
fi

service=$1

home=/opt/$(ls /opt | grep alluxio)
cd ${home}

# List of environment variables starting with ALLUXIO_ which
# don't have corresponding configuration keys.
special_env_vars=(
  ALLUXIO_CLASSPATH
  ALLUXIO_HOSTNAME
  ALLUXIO_JARS
  ALLUXIO_JAVA_OPTS
  ALLUXIO_JOB_MASTER_JAVA_OPTS
  ALLUXIO_JOB_WORKER_JAVA_OPTS
  ALLUXIO_MASTER_JAVA_OPTS
  ALLUXIO_PROXY_JAVA_OPTS
  ALLUXIO_RAM_FOLDER
  ALLUXIO_USER_JAVA_OPTS
  ALLUXIO_WORKER_JAVA_OPTS
  ALLUXIO_LICENSE_BASE64
)

for keyvaluepair in $(env | grep "ALLUXIO_"); do
  # split around the "="
  key=$(echo ${keyvaluepair} | cut -d= -f1)
  value=$(echo ${keyvaluepair} | cut -d= -f2)
  if [[ ! "${special_env_vars[*]}" =~ "${key}" ]]; then
    confkey=$(echo ${key} | sed "s/_/./g" | tr '[:upper:]' '[:lower:]')
    echo "${confkey}=${value}" >> conf/alluxio-site.properties
  fi
done

if [[ -n "${ALLUXIO_LICENSE_BASE64}" ]]; then
  echo "${ALLUXIO_LICENSE_BASE64}" | base64 -d > license.json
fi

case ${service,,} in
  master)
    bin/alluxio format
    integration/docker/bin/alluxio-job-master.sh &
    integration/docker/bin/alluxio-master.sh &
    wait -n
    ;;
  worker)
    export ALLUXIO_RAM_FOLDER=${ALLUXIO_RAM_FOLDER:-/dev/shm}
    bin/alluxio formatWorker
    integration/docker/bin/alluxio-job-worker.sh &
    integration/docker/bin/alluxio-worker.sh &
    wait -n
    ;;
  proxy)
    integration/docker/bin/alluxio-proxy.sh
    ;;
  *)
    echo 'expected "master", "worker", or "proxy"';
    exit 1
    ;;
esac
