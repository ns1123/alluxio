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

USAGE="Usage: alluxio-workers.sh command..."

# if no args specified, show usage
if [[ $# -le 0 ]]; then
  echo "${USAGE}"
  exit 1
fi

DEFAULT_LIBEXEC_DIR="${BIN}/../../libexec"
ALLUXIO_LIBEXEC_DIR="${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}"
. "${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh"

HOSTLIST="${ALLUXIO_CONF_DIR}/workers"

for worker in $(cat "${HOSTLIST}" | sed  "s/#.*$//;/^$/d"); do
  echo "Connecting to ${worker} as ${USER} ..."
  if [[ -n "${ALLUXIO_SSH_FOREGROUND}" ]]; then
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -t ${worker} ${LAUNCHER} $"${@// /\\ }" 2>&1
  else
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -t ${worker} ${LAUNCHER} $"${@// /\\ }" 2>&1 &
  fi
  if [[ -n "${ALLUXIO_WORKER_SLEEP}" ]]; then
    sleep "${ALLUXIO_WORKER_SLEEP}"
  fi
done

wait
