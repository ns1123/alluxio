#!/usr/bin/env bash

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$0" )"; pwd)

# start alluxio job service

Usage="Usage: alluxio-start.sh [-hNw] WHAT
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

-h  display this help."

ensure_dirs() {
  if [ ! -d "$ALLUXIO_LOGS_DIR" ]; then
    echo "ALLUXIO_LOGS_DIR: $ALLUXIO_LOGS_DIR"
    mkdir -p $ALLUXIO_LOGS_DIR
  fi
}

get_env() {
  DEFAULT_LIBEXEC_DIR="${BIN}"/../../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  . $ALLUXIO_LIBEXEC_DIR/alluxio-config.sh
}

stop() {
  ${BIN}/alluxio-stop.sh all
}


start_master() {
  MASTER_ADDRESS=$ALLUXIO_MASTER_ADDRESS
  if [ -z $ALLUXIO_MASTER_ADDRESS ] ; then
    MASTER_ADDRESS=localhost
  fi

  if [[ -z $ALLUXIO_MASTER_JAVA_OPTS ]] ; then
    ALLUXIO_MASTER_JAVA_OPTS=$ALLUXIO_JAVA_OPTS
  fi

  if [ "${1}" == "-f" ] ; then
    $LAUNCHER ${BIN}/../../bin/alluxio format
  fi

  echo "Starting job master @ $MASTER_ADDRESS. Logging to $ALLUXIO_LOGS_DIR"
  (nohup $JAVA -cp $CLASSPATH -Dalluxio.home=$ALLUXIO_HOME -Dalluxio.logs.dir=$ALLUXIO_LOGS_DIR -Dalluxio.logger.type="MASTER_LOGGER" -Dalluxio.accesslogger.type="MASTER_ACCESS_LOGGER" -Dlog4j.configuration=file:$ALLUXIO_CONF_DIR/log4j.properties $ALLUXIO_MASTER_JAVA_OPTS alluxio.master.AlluxioJobMaster > $ALLUXIO_LOGS_DIR/job_master.out 2>&1) &
}

start_worker() {
  if [[ -z $ALLUXIO_WORKER_JAVA_OPTS ]] ; then
    ALLUXIO_WORKER_JAVA_OPTS=$ALLUXIO_JAVA_OPTS
  fi

  echo "Starting job worker @ `hostname -f`. Logging to $ALLUXIO_LOGS_DIR"
  (nohup $JAVA -cp $CLASSPATH -Dalluxio.home=$ALLUXIO_HOME -Dalluxio.logs.dir=$ALLUXIO_LOGS_DIR -Dalluxio.logger.type="WORKER_LOGGER" -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" -Dlog4j.configuration=file:$ALLUXIO_CONF_DIR/log4j.properties $ALLUXIO_WORKER_JAVA_OPTS alluxio.worker.AlluxioJobWorker > $ALLUXIO_LOGS_DIR/job_worker.out 2>&1 ) &
}

restart_worker() {
  if [[ -z $ALLUXIO_WORKER_JAVA_OPTS ]] ; then
    ALLUXIO_WORKER_JAVA_OPTS=$ALLUXIO_JAVA_OPTS
  fi

  RUN=`ps -ef | grep "alluxio.worker.AlluxioJobWorker" | grep "java" | wc | cut -d" " -f7`
  if [[ $RUN -eq 0 ]] ; then
    echo "Restarting worker @ `hostname -f`. Logging to $ALLUXIO_LOGS_DIR"
    (nohup $JAVA -cp $CLASSPATH -Dalluxio.home=$ALLUXIO_HOME -Dalluxio.logs.dir=$ALLUXIO_LOGS_DIR -Dalluxio.logger.type="WORKER_LOGGER" -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" -Dlog4j.configuration=file:$ALLUXIO_CONF_DIR/log4j.properties $ALLUXIO_WORKER_JAVA_OPTS alluxio.worker.AluxioJobWorker > $ALLUXIO_LOGS_DIR/job_worker.out 2>&1) &
  fi
}

run_safe() {
  while [ 1 ]
  do
    RUN=`ps -ef | grep "alluxio.master.AlluxioJobMaster" | grep "java" | wc | cut -d" " -f7`
    if [[ $RUN -eq 0 ]] ; then
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
      echo -e "$Usage"
      exit 0
      ;;
    N)
      killonstart="no"
      ;;
    w)
      wait="true"
      ;;
    *)
      echo -e "$Usage"
      exit 1
      ;;
  esac
done

shift $((OPTIND-1))

WHAT=$1

if [ -z "${WHAT}" ]; then
  echo "Error: no WHAT specified"
  echo -e "$Usage"
  exit 1
fi

# get environment
get_env

# ensure log/data dirs
ensure_dirs

case "${WHAT}" in
  all)
    if [ "${killonstart}" != "no" ]; then
      stop ${BIN}
    fi
    start_master
    sleep 2
    $LAUNCHER ${BIN}/alluxio-workers.sh ${BIN}/alluxio-start.sh worker
    ;;
  local)
    if [ "${killonstart}" != "no" ]; then
      stop ${BIN}
      sleep 1
    fi
    start_master
    sleep 2
    start_worker
    ;;
  master)
    start_master
    ;;
  worker)
    start_worker
    ;;
  safe)
    run_safe
    ;;
  workers)
    $LAUNCHER ${BIN}/alluxio-workers.sh ${BIN}/alluxio-start.sh worker $ALLUXIO_MASTER_ADDRESS
    ;;
  restart_worker)
    restart_worker
    ;;
  restart_workers)
    $LAUNCHER ${BIN}/alluxio-workers.sh ${BIN}/alluxio-start.sh restart_worker
    ;;
  *)
    echo "Error: Invalid WHAT: $WHAT"
    echo -e "$Usage"
    exit 1
esac
sleep 2

if [[ "${wait}" ]]; then
  wait
fi
