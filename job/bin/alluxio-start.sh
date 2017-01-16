#!/usr/bin/env bash
#
# Copyright (c) 2016 Alluxio, Inc. All rights reserved.
#
# This software and all information contained herein is confidential and proprietary to Alluxio,
# and is protected by copyright and other applicable laws in the United States and other
# jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
# the express written permission of Alluxio.
#


LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$0" )"; pwd)

# start alluxio job service

USAGE="Usage: alluxio-start.sh [-hNw] WHAT
Where WHAT is one of:
  all \t\tStart master and all workers.
  local\t\t\tStart a master and worker locally
  master\t\tStart the master on this node
  safe\t\t\tScript will run continuously and start the master if it's not running
  worker \t\tStart a worker on this node
  workers \t\tStart workers on worker nodes
  restart_worker\tRestart a failed worker on this node
  restart_workers\tRestart any failed workers on worker nodes

-N  do not try to kill prior running masters and/or workers in all or local

-w  wait for processes to end before returning

-h  display this help.

Supported environment variables:

ALLUXIO_JOB_WORKER_COUNT - identifies how many job workers to start per node (default = 1)
"

ensure_dirs() {
  if [[ ! -d "${ALLUXIO_LOGS_DIR}" ]]; then
    echo "ALLUXIO_LOGS_DIR: ${ALLUXIO_LOGS_DIR}"
    mkdir -p "${ALLUXIO_LOGS_DIR}"
  fi
}

get_env() {
  DEFAULT_LIBEXEC_DIR="${BIN}"/../../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . "${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh"
}

stop() {
  "${BIN}/alluxio-stop.sh" all
}


start_master() {
  if [[ -z ${ALLUXIO_MASTER_JAVA_OPTS} ]] ; then
    ALLUXIO_MASTER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  if [ "$1" == "-f" ] ; then
    ${LAUNCHER} "${BIN}/../../bin/alluxio" format
  fi

  echo "Starting job master @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} -Dalluxio.home=${ALLUXIO_HOME} -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR} -Dalluxio.logger.type="MASTER_LOGGER" -Dalluxio.accesslogger.type="MASTER_ACCESS_LOGGER" -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties ${ALLUXIO_MASTER_JAVA_OPTS} alluxio.master.AlluxioJobMaster > ${ALLUXIO_LOGS_DIR}/job_master.out 2>&1) &
}

start_worker() {
  if [[ -z ${ALLUXIO_WORKER_JAVA_OPTS} ]] ; then
    ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  echo "Starting job worker #1 @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} -Dalluxio.home=${ALLUXIO_HOME} -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR} -Dalluxio.logger.type="WORKER_LOGGER" -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties ${ALLUXIO_WORKER_JAVA_OPTS} alluxio.worker.AlluxioJobWorker > ${ALLUXIO_LOGS_DIR}/job_worker.out 2>&1 ) &
  local nworkers=${1:-1}
  for (( c = 1; c < ${nworkers}; c++ )); do
    echo "Starting job worker #$((c+1)) @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup ${JAVA} -cp ${CLASSPATH} -Dalluxio.home=${ALLUXIO_HOME} -Dalluxio.job.worker.rpc.port=0 -Dalluxio.job.worker.web.port=0 -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR} -Dalluxio.logger.type="WORKER_LOGGER" -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties ${ALLUXIO_WORKER_JAVA_OPTS} alluxio.worker.AlluxioJobWorker > ${ALLUXIO_LOGS_DIR}/job_worker.out 2>&1 ) &
  done
}

restart_worker() {
  if [[ -z ${ALLUXIO_WORKER_JAVA_OPTS} ]] ; then
    ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  local run=$(ps -ef | grep "alluxio.worker.AlluxioJobWorker" | grep "java" | wc | cut -d" " -f7)
  if [[ ${run} -eq 0 ]] ; then
    echo "Restarting job worker #1 @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup ${JAVA} -cp ${CLASSPATH} -Dalluxio.home=${ALLUXIO_HOME} -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR} -Dalluxio.logger.type="WORKER_LOGGER" -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties ${ALLUXIO_WORKER_JAVA_OPTS} alluxio.worker.AlluxioJobWorker > ${ALLUXIO_LOGS_DIR}/job_worker.out 2>&1 ) &
    local nworkers=${1:-1}
    for (( c = 1; c < ${nworkers}; c++ )); do
      echo "Restarting job worker #$((c+1)) @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
      (nohup ${JAVA} -cp ${CLASSPATH} -Dalluxio.home=${ALLUXIO_HOME} -Dalluxio.job.worker.rpc.port=0 -Dalluxio.job.worker.web.port=0 -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR} -Dalluxio.logger.type="WORKER_LOGGER" -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties ${ALLUXIO_WORKER_JAVA_OPTS} alluxio.worker.AlluxioJobWorker > ${ALLUXIO_LOGS_DIR}/job_worker.out 2>&1 ) &
    done
  fi
}

run_safe() {
  while [ 1 ]
  do
    local run=$(ps -ef | grep "alluxio.master.AlluxioJobMaster" | grep "java" | wc | cut -d" " -f7)
    if [[ "${run}" -eq 0 ]] ; then
      echo "Restarting the Alluxio job master..."
      start_master
    fi
    echo "Alluxio job service is running... "
    sleep 2
  done
}

while getopts "hNw" o; do
  case "${o}" in
    h)
      echo -e "${USAGE}"
      exit 0
      ;;
    N)
      killonstart="no"
      ;;
    w)
      wait="true"
      ;;
    *)
      echo -e "${USAGE}"
      exit 1
      ;;
  esac
done

shift $((OPTIND-1))

WHAT=$1

if [[ -z "${WHAT}" ]]; then
  echo "Error: no WHAT specified"
  echo -e "${USAGE}"
  exit 1
fi

# get environment
get_env

# ensure log/data dirs
ensure_dirs

NWORKERS=${ALLUXIO_JOB_WORKER_COUNT:-1}

case "${WHAT}" in
  all)
    if [[ "${killonstart}" != "no" ]]; then
      stop "${BIN}"
    fi
    start_master
    sleep 2
    ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" worker ${NWORKERS}
    ;;
  local)
    if [[ "${killonstart}" != "no" ]]; then
      stop "${BIN}"
      sleep 1
    fi
    start_master
    sleep 2
    start_worker ${NWORKERS}
    ;;
  master)
    start_master
    ;;
  worker)
    if [[ -n $2 ]]; then
      NWORKERS=$2
    fi
    start_worker ${NWORKERS}
    ;;
  safe)
    run_safe
    ;;
  workers)
    ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" worker "${ALLUXIO_MASTER_HOSTNAME}"
    ;;
  restart_worker)
    if [[ -n $2 ]]; then
      NWORKERS=$2
    fi
    restart_worker ${NWORKERS}
    ;;
  restart_workers)
    ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" restart_worker ${NWORKERS}
    ;;
  *)
    echo "Error: Invalid WHAT: ${WHAT}"
    echo -e "${USAGE}"
    exit 1
esac
sleep 2

if [[ "${wait}" ]]; then
  wait
fi
