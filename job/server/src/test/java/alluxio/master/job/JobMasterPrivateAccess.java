/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.job.meta.MasterWorkerInfo;

import org.powermock.reflect.Whitebox;

/**
 * Class which provides access to private state of {@link JobMaster}.
 */
public final class JobMasterPrivateAccess {
  /**
   * Checks whether a worker with the given worker id is registered with the given job master.
   *
   * @param master the job master
   * @param workerId the worker id
   * @return true if the worker has registered, false otherwise
   */
  public static boolean isWorkerRegistered(JobMaster master, long workerId) {
    IndexedSet<MasterWorkerInfo> workers = Whitebox.getInternalState(master, "mWorkers");
    IndexDefinition<MasterWorkerInfo> idIndex = Whitebox.getInternalState(master, "mIdIndex");
    synchronized (workers) {
      MasterWorkerInfo workerInfo = workers.getFirstByField(idIndex, workerId);
      return workerInfo != null;
    }
  }
}
