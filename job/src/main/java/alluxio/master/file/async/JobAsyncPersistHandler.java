/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.file.async;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.job.persist.PersistConfig;
import alluxio.master.AlluxioMaster;
import alluxio.master.Master;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.job.JobMaster;
import alluxio.thrift.PersistFile;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The async persist handler that schedules the async persistence of file by sending a persistence
 * request to the job service.
 */
public final class JobAsyncPersistHandler implements AsyncPersistHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private JobMaster mJobMaster;

  private JobMaster getJobMaster() {
    for (Master master : AlluxioMaster.get().getAdditionalMasters()) {
      if (master instanceof JobMaster) {
        return (JobMaster) master;
      }
    }
    LOG.error("JobMaster is not registered in Alluxio Master");
    return null;
  }

  /**
   * Constructs a new instance of {@link JobAsyncPersistHandler}.
   *
   * @param view the view of {@link FileSystemMasterView}
   */
  public JobAsyncPersistHandler(FileSystemMasterView view) {}

  @Override
  public synchronized void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException {
    if (mJobMaster == null) {
      mJobMaster = getJobMaster();
    }
    LOG.info("scheduled async persist of file " + path);
    mJobMaster.runJob(new PersistConfig(path.getPath(), true));
  }

  @Override
  public List<PersistFile> pollFilesToPersist(long workerId) {
    // the files are persisted by job service, so this method is not used
    return Lists.newArrayList();
  }
}
