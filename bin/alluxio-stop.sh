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
#   all     \tStop master and all proxies and workers.
#   local   \tStop local master, proxy, and worker.
#   master  \tStop local master.
#   proxy   \tStop local proxy.
#   proxies \tStop proxies on worker nodes.
#   worker  \tStop local worker.
#   workers \tStop workers on worker nodes.
#
# -h  display this help."
# ALLUXIO CS WITH
USAGE="Usage: alluxio-stop.sh [-h] [component]
Where component is one of:
  all               \tStop master and all proxies and workers.
  local             \tStop local master, proxy, and worker.
  job_master        \tStop local job master.
  job_worker        \tStop local job worker.
  job_workers       \tStop job workers on worker nodes.
  master            \tStop local master.
  proxy             \tStop local proxy.
  proxies           \tStop proxies on worker nodes.
  secondary_master  \tStop local secondary master.
  secondary_masters \tStop secondary masters on the secondary master nodes.
  worker            \tStop local worker.
  workers           \tStop workers on worker nodes.

-h  display this help."
# ALLUXIO CS END

stop_master() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioMaster"
}
# ALLUXIO CS ADD

stop_job_master() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioJobMaster"
}

stop_job_worker() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioJobWorker"
}

stop_job_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioJobWorker"
}
# ALLUXIO CS END

stop_proxy() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.proxy.AlluxioProxy"
}

stop_secondary_master() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioSecondaryMaster"
}

stop_worker() {
  ${LAUNCHER} "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioWorker"
}

stop_proxies() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio" "killAll" "alluxio.proxy.AlluxioProxy"
}

stop_secondary_masters() {
  ${LAUNCHER} "${BIN}/alluxio-secondary-masters.sh" "${BIN}/alluxio" "killAll" "alluxio.master.AlluxioSecondaryMaster"
}

stop_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio" "killAll" "alluxio.worker.AlluxioWorker"
}

WHAT=${1:--h}

case "${WHAT}" in
  all)
    stop_proxies
    # ALLUXIO CS ADD
    stop_job_workers
    # ALLUXIO CS END
    stop_workers
    stop_secondary_masters
    stop_secondary_master
    stop_proxy
    # ALLUXIO CS ADD
    stop_job_master
    # ALLUXIO CS END
    stop_master
    ;;
  local)
    stop_proxy
    # ALLUXIO CS ADD
    stop_job_worker
    stop_job_master
    # ALLUXIO CS END
    stop_worker
    stop_master
    stop_secondary_master
    ;;
# ALLUXIO CS ADD
  job_master)
    stop_job_master
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
  proxy)
    stop_proxy
    ;;
  secondary_master)
    stop_secondary_master
    ;;
  secondary_masters)
    stop_secondary_masters
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
