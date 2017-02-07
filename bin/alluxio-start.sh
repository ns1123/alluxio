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

#start up alluxio

# ALLUXIO CS REPLACE
#USAGE="Usage: alluxio-start.sh [-hNw] ACTION [MOPT] [-f]
#Where ACTION is one of:
#  all [MOPT]     \tStart master and all proxies and workers.
#  local [MOPT]   \tStart a master, proxy, and worker locally.
#  master         \tStart the master on this node.
#  proxy          \tStart the proxy on this node.
#  proxies        \tStart proxies on worker nodes.
#  safe           \tScript will run continuously and start the master if it's not running.
#  worker [MOPT]  \tStart a worker on this node.
#  workers [MOPT] \tStart workers on worker nodes.
#  restart_worker \tRestart a failed worker on this node.
#  restart_workers\tRestart any failed workers on worker nodes.
#
#MOPT (Mount Option) is one of:
#  Mount    \tMount the configured RamFS. Notice: this will format the existing RamFS.
#  SudoMount\tMount the configured RamFS using sudo.
#           \tNotice: this will format the existing RamFS.
#  NoMount  \tDo not mount the configured RamFS.
#           \tNotice: to avoid sudo requirement but using tmpFS in Linux,
#             set ALLUXIO_RAM_FOLDER=/dev/shm on each worker and use NoMount.
#  SudoMount is assumed if MOPT is not specified.
#
#-f  format Journal, UnderFS Data and Workers Folder on master
#-N  do not try to kill prior running masters and/or workers in all or local
#-w  wait for processes to end before returning
#-h  display this help."
# ALLUXIO CS WITH
USAGE="Usage: alluxio-start.sh [-hNw] ACTION [MOPT] [-f]
Where ACTION is one of:
  all [MOPT]     \tStart job master, master and all job workers, proxies and workers.
  local [MOPT]   \tStart a job master, job worker, master, proxy, and worker locally.
  job_master     \tStart the job master on this node.
  job_worker     \tStart a job worker on this node.
  job_workers    \tStart job workers on worker nodes.
  master         \tStart the master on this node.
  proxy          \tStart the proxy on this node.
  proxies        \tStart proxies on worker nodes.
  safe           \tScript will run continuously and start the master if it's not running.
  worker [MOPT]  \tStart a worker on this node.
  workers [MOPT] \tStart workers on worker nodes.
  restart_worker \tRestart a failed worker on this node.
  restart_workers\tRestart any failed workers on worker nodes.

MOPT (Mount Option) is one of:
  Mount    \tMount the configured RamFS. Notice: this will format the existing RamFS.
  SudoMount\tMount the configured RamFS using sudo.
           \tNotice: this will format the existing RamFS.
  NoMount  \tDo not mount the configured RamFS.
           \tNotice: to avoid sudo requirement but using tmpFS in Linux,
             set ALLUXIO_RAM_FOLDER=/dev/shm on each worker and use NoMount.
  SudoMount is assumed if MOPT is not specified.

-f  format Journal, UnderFS Data and Workers Folder on master
-N  do not try to kill prior running masters and/or workers in all or local
-w  wait for processes to end before returning
-h  display this help."
# ALLUXIO CS END

ensure_dirs() {
  if [[ ! -d "${ALLUXIO_LOGS_DIR}" ]]; then
    echo "ALLUXIO_LOGS_DIR: ${ALLUXIO_LOGS_DIR}"
    mkdir -p ${ALLUXIO_LOGS_DIR}
  fi
}

get_env() {
  DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh
}

# Pass ram folder to check as $1
# Return 0 if ram folder is mounted as tmpfs or ramfs, 1 otherwise
is_ram_folder_mounted() {
  local mounted_fs=""
  if [[ $(uname -s) == Darwin ]]; then
    mounted_fs=$(mount -t "hfs" | grep '/Volumes/' | cut -d " " -f 3)
  else
    mounted_fs=$(mount -t "tmpfs,ramfs" | cut -d " " -f 3)
  fi

  for fs in ${mounted_fs}; do
    if [[ "${1}" == "${fs}" || "${1}" =~ ^"${fs}"\/.* ]]; then
      return 0
    fi
  done

  return 1
}

check_mount_mode() {
  case $1 in
    Mount);;
    SudoMount);;
    NoMount)
      local tier_alias=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.alias)
      local tier_path=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.dirs.path)
      if [[ ${tier_alias} != "MEM" ]]; then
        # if the top tier is not MEM, skip check
        return
      fi
      is_ram_folder_mounted "${tier_path}"
      if [[ $? -ne 0 ]]; then
        if [[ $(uname -s) == Darwin ]]; then
          # Assuming Mac OS X
          echo "ERROR: NoMount is not supported on Mac OS X." >&2
          echo -e "${USAGE}" >&2
          exit 1
        fi
      fi
      if [[ "${tier_path}" =~ ^"/dev/shm"\/{0,1}$ ]]; then
        echo "WARNING: Using tmpFS does not guarantee data to be stored in memory."
        echo "WARNING: Check vmstat for memory statistics (e.g. swapping)."
      fi
      ;;
    *)
      if [[ -z $1 ]]; then
        echo "This command requires a mount mode be specified" >&2
      else
        echo "Invalid mount mode: $1" >&2
      fi
      echo -e "${USAGE}" >&2
      exit 1
  esac
}

# pass mode as $1
do_mount() {
  MOUNT_FAILED=0
  case "$1" in
    Mount|SudoMount)
      ${LAUNCHER} "${BIN}/alluxio-mount.sh" $1
      MOUNT_FAILED=$?
      ;;
    NoMount)
      ;;
    *)
      echo "This command requires a mount mode be specified" >&2
      echo -e "${USAGE}" >&2
      exit 1
  esac
}

stop() {
  ${BIN}/alluxio-stop.sh all
}

# ALLUXIO CS ADD
start_job_master() {
  if [[ -z ${ALLUXIO_JOB_MASTER_JAVA_OPTS} ]] ; then
    ALLUXIO_JOB_MASTER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  if [[ "$1" == "-f" ]]; then
    ${LAUNCHER} "${BIN}/alluxio" format
  fi

  echo "Starting job master @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} \
   ${ALLUXIO_JOB_MASTER_JAVA_OPTS} \
   alluxio.master.AlluxioJobMaster > ${ALLUXIO_LOGS_DIR}/job_master.out 2>&1) &
}

start_job_worker() {
  if [[ -z ${ALLUXIO_JOB_WORKER_JAVA_OPTS} ]] ; then
    ALLUXIO_JOB_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  echo "Starting job worker @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} \
   ${ALLUXIO_JOB_WORKER_JAVA_OPTS} \
   alluxio.worker.AlluxioJobWorker > ${ALLUXIO_LOGS_DIR}/job_worker.out 2>&1) &
  ALLUXIO_JOB_WORKER_JAVA_OPTS+=" -Dalluxio.job.worker.rpc.port=0 -Dalluxio.job.worker.web.port=0"
  local nworkers=${1:-1}
  for (( c = 1; c < ${nworkers}; c++ )); do
    echo "Starting job worker #$((c+1)) @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup ${JAVA} -cp ${CLASSPATH} \
     ${ALLUXIO_JOB_WORKER_JAVA_OPTS} \
     alluxio.worker.AlluxioJobWorker > ${ALLUXIO_LOGS_DIR}/job_worker.out 2>&1) &
  done
}

start_job_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "job_worker"
}
# ALLUXIO CS END

start_master() {
  if [[ -z ${ALLUXIO_MASTER_JAVA_OPTS} ]]; then
    ALLUXIO_MASTER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  if [[ "$1" == "-f" ]]; then
    ${LAUNCHER} ${BIN}/alluxio format
  fi

  echo "Starting master @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} \
   ${ALLUXIO_MASTER_JAVA_OPTS} \
   alluxio.master.AlluxioMaster > ${ALLUXIO_LOGS_DIR}/master.out 2>&1) &
}

start_proxy() {
  if [[ -z ${ALLUXIO_PROXY_JAVA_OPTS} ]]; then
    ALLUXIO_PROXY_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  echo "Starting proxy @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} \
   ${ALLUXIO_PROXY_JAVA_OPTS} \
   alluxio.proxy.AlluxioProxy > ${ALLUXIO_LOGS_DIR}/proxy.out 2>&1) &
}

start_worker() {
  do_mount $1
  if  [ ${MOUNT_FAILED} -ne 0 ] ; then
    echo "Mount failed, not starting worker" >&2
    exit 1
  fi

  if [[ -z ${ALLUXIO_WORKER_JAVA_OPTS} ]]; then
    ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  echo "Starting worker @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} \
   ${ALLUXIO_WORKER_JAVA_OPTS} \
   alluxio.worker.AlluxioWorker > ${ALLUXIO_LOGS_DIR}/worker.out 2>&1 ) &
}

restart_worker() {
  if [[ -z ${ALLUXIO_WORKER_JAVA_OPTS} ]]; then
    ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  RUN=$(ps -ef | grep "alluxio.worker.AlluxioWorker" | grep "java" | wc | cut -d" " -f7)
  if [[ ${RUN} -eq 0 ]]; then
    echo "Restarting worker @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup ${JAVA} -cp ${CLASSPATH} \
     ${ALLUXIO_WORKER_JAVA_OPTS} \
     alluxio.worker.AlluxioWorker > ${ALLUXIO_LOGS_DIR}/worker.out 2>&1) &
  fi
}

restart_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "restart_worker"
}

start_proxies() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "proxy"
}

start_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "worker" $1
}

run_safe() {
  while [ 1 ]
  do
    RUN=$(ps -ef | grep "alluxio.master.AlluxioMaster" | grep "java" | wc | cut -d" " -f7)
    if [[ ${RUN} -eq 0 ]]; then
      echo "Restarting the system master..."
      start_master
    fi
    echo "Alluxio is running... "
    sleep 2
  done
}

main() {
  # get environment
  get_env

  # ensure log/data dirs
  ensure_dirs

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
        echo -e "${USAGE}" >&2
        exit 1
        ;;
    esac
  done

  shift $((${OPTIND} - 1))

  ACTION=$1
  if [[ -z "${ACTION}" ]]; then
    echo "Error: no ACTION specified" >&2
    echo -e "${USAGE}" >&2
    exit 1
  fi
  shift

  MOPT=$1
  # Set MOPT.
  case "${ACTION}" in
    all|worker|workers|local)
      if [[ -z "${MOPT}" || "${MOPT}" == "-f" ]]; then
        MOPT="SudoMount"
      else
        shift
      fi
      check_mount_mode "${MOPT}"
      ;;
    *)
      MOPT=""
      ;;
  esac

  FORMAT=$1
  if [[ ! -z "${FORMAT}" && "${FORMAT}" != "-f" ]]; then
    echo -e "${USAGE}" >&2
    exit 1
  fi

  case "${ACTION}" in
    all)
      if [[ "${killonstart}" != "no" ]]; then
        stop
      fi
      start_master "${FORMAT}"
      # ALLUXIO CS ADD
      start_job_master
      # ALLUXIO CS END
      start_proxy
      sleep 2
      start_workers "${MOPT}"
      # ALLUXIO CS ADD
      start_job_workers
      # ALLUXIO CS END
      start_proxies
      ;;
    local)
      if [[ "${killonstart}" != "no" ]]; then
        stop
        sleep 1
      fi
      start_master "${FORMAT}"
      sleep 2
      start_worker "${MOPT}"
      # ALLUXIO CS ADD
      start_job_master
      start_job_worker
      # ALLUXIO CS END
      start_proxy
      ;;
# ALLUXIO CS ADD
    job_master)
      start_job_master "${FORMAT}"
      ;;
    job_worker)
      start_job_worker
      ;;
    job_workers)
      start_job_workers
      ;;
# ALLUXIO CS END
    master)
      start_master "${FORMAT}"
      ;;
    proxy)
      start_proxy
      ;;
    proxies)
      start_proxies
      ;;
    restart_worker)
      restart_worker
      ;;
    restart_workers)
      restart_workers
      ;;
    safe)
      run_safe
      ;;
    worker)
      start_worker "${MOPT}"
      ;;
    workers)
      start_workers "${MOPT}"
      ;;
    *)
    echo "Error: Invalid ACTION: ${ACTION}" >&2
    echo -e "${USAGE}" >&2
    exit 1
  esac
  sleep 2

  if [[ "${wait}" ]]; then
    wait
  fi
}

main "$@"
