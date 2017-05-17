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
#   all               \tStop master and all proxies and workers.
#   local             \tStop local master, proxy, and worker.
#   master            \tStop local master.
#   proxy             \tStop local proxy.
#   proxies           \tStop proxies on worker nodes.
#   secondary_master  \tStop local secondary master.
#   secondary_masters \tStop secondary masters on the secondary master nodes.
#   worker            \tStop local worker.
#   workers           \tStop workers on worker nodes.
#
# -h  display this help."
# ALLUXIO CS WITH
USAGE="Usage: alluxio-stop.sh [-h] [component]
Where component is one of:
<<<<<<< HEAD
  all               \tStop master and all proxies and workers.
  local             \tStop local master, proxy, and worker.
  job_master        \tStop local job master.
  job_worker        \tStop local job worker.
  job_workers       \tStop job workers on worker nodes.
||||||| merged common ancestors
  all               \tStop master and all proxies and workers.
  local             \tStop local master, proxy, and worker.
=======
  all               \tStop all masters, proxies, and workers.
  local             \tStop all processes locally.
>>>>>>> 7ef33355d76f0f51b495ab97fe59aa88449e9c7c
  master            \tStop local master.
  masters           \tStop masters on master nodes.
  proxy             \tStop local proxy.
  proxies           \tStop proxies on master and worker nodes.
  worker            \tStop local worker.
  workers           \tStop workers on worker nodes.

-h  display this help."
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
<<<<<<< HEAD
    stop_secondary_masters
    stop_secondary_master
    stop_proxy
    # ALLUXIO CS ADD
    stop_job_master
    # ALLUXIO CS END
    stop_master
||||||| merged common ancestors
    stop_secondary_masters
    stop_secondary_master
    stop_proxy
    stop_master
=======
    stop_masters
>>>>>>> 7ef33355d76f0f51b495ab97fe59aa88449e9c7c
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
