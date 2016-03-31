/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job;

import alluxio.Constants;
import alluxio.worker.Worker;
import alluxio.worker.WorkerFactory;
import alluxio.worker.block.BlockWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link JobManagerWorker} instance.
 */
@ThreadSafe
public class JobManagerWorkerFactory implements WorkerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public Worker create(List<? extends Worker> workers) {
    LOG.info("Creating {} ", JobManagerWorker.class.getName());

    for (Worker worker : workers) {
      if (worker instanceof BlockWorker) {
        LOG.info("{} is created", JobManagerWorker.class.getName());
        return new JobManagerWorker(((BlockWorker) worker));
      }
    }
    LOG.error("Fail to create {} due to missing {}", JobManagerWorker.class.getName(),
        BlockWorker.class.getName());
    return null;
  }
}
