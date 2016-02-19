/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.jobmanager;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.Constants;

/**
 * Constants used in Job Manager.
 */
@ThreadSafe
public final class AlluxioEEConstants extends Constants {
  public static final String JOB_MANAGER_MASTER_WORKER_SERVICE_NAME = "JobManagerMasterWorker";

  public static final long JOB_MANAGER_MASTER_WORKER_SERVICE_VERSION = 1;

  public static final String JOB_MANAGER_MASTER_NAME = "JobManagerMaster";

  public static final String JOB_MANAGER_MASTER_WORKER_HEARTBEAT_INTERVAL_MS =
      "alluxio.job.manager.master.worker.heartbeat.interval.ms";
  public static final String JOB_MANAGER_MASTER_CLIENT_SERVICE_NAME = "JobManagerMasterClient";
  public static final int JOB_MANAGER_MASTER_CLIENT_SERVICE_VERSION = 1;
}
