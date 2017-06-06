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

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$0" )"; pwd)

# ALLUXIO CS REPLACE
# USAGE="Usage: alluxio-stop.sh [-h] [component]
# Where component is one of:
#   all               \tStop all masters, proxies, and workers.
#   local             \tStop all processes locally.
#   master            \tStop local master.
#   masters           \tStop masters on master nodes.
#   proxy             \tStop local proxy.
#   proxies           \tStop proxies on master and worker nodes.
#   worker            \tStop local worker.
#   workers           \tStop workers on worker nodes.
#
# -h  display this help."
# ALLUXIO CS WITH
USAGE="Usage: alluxio-stop.sh [-h] [component]
Where component is one of:
  all               \tStop all masters, proxies, and workers.
  job_master        \tStop local job master.
  job_masters       \tStop job masters on master nodes.
  job_worker        \tStop local job worker.
  job_workers       \tStop job workers on worker nodes.
  local             \tStop all processes locally.
  master            \tStop local master.
  masters           \tStop masters on master nodes.
  proxy             \tStop local proxy.
  proxies           \tStop proxies on master and worker nodes.
  worker            \tStop local worker.
  workers           \tStop workers on worker nodes.

-h  display this help."
# ALLUXIO CS END

# ALLUXIO CS ADD
stop_job_master() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioJobMaster"
}

stop_job_masters() {
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio" "${BIN}/alluxio-stop.sh" "job_master"
}

stop_job_worker() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioJobWorker"
}

stop_job_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio" "${BIN}/alluxio-stop.sh" "job_worker"
}

# ALLUXIO CS END
stop_master() {
  if [[ ${ALLUXIO_MASTER_SECONDARY} == "true" ]]; then
    ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioSecondaryMaster"
  else
    ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioMaster"
  fi
}

stop_masters() {
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio-stop.sh" "master"
}

stop_proxy() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.proxy.AlluxioProxy"
}

stop_proxies() {
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio-stop.sh" "proxy"
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-stop.sh" "proxy"
}

stop_worker() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioWorker"
}

stop_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-stop.sh" "worker"
}


WHAT=${1:--h}

case "${WHAT}" in
  all)
    stop_proxies
    # ALLUXIO CS ADD
    stop_job_workers
    # ALLUXIO CS END
    stop_workers
    # ALLUXIO CS ADD
    stop_job_masters
    # ALLUXIO CS END
    stop_masters
    ;;
  local)
    stop_proxy
    # ALLUXIO CS ADD
    stop_job_worker
    stop_job_master
    # ALLUXIO CS END
    stop_worker
    ALLUXIO_MASTER_SECONDARY=true
    stop_master
    ALLUXIO_MASTER_SECONDARY=false
    stop_master
    ;;
# ALLUXIO CS ADD
  job_master)
    stop_job_master
    ;;
  job_masters)
    stop_job_masters
    ;;
  job_worker)
    stop_job_worker
    ;;
  job_workers)
    stop_job_workers
    ;;
# ALLUXIO CS END
  master)
    stop_master
    ;;
  masters)
    stop_masters
    ;;
  proxy)
    stop_proxy
    ;;
  proxies)
    stop_proxies
    ;;
  worker)
    stop_worker
    ;;
  workers)
    stop_workers
    ;;
  -h)
    echo -e "${USAGE}"
    exit 0
    ;;
  *)
    echo "Error: Invalid component: ${WHAT}" >&2
    echo -e "${USAGE}" >&2
    exit 1
    ;;
esac
