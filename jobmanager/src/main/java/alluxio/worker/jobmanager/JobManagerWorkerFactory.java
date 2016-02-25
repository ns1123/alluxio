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

package alluxio.worker.jobmanager;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Constants;
import alluxio.worker.Worker;
import alluxio.worker.WorkerFactory;
import alluxio.worker.block.BlockWorker;

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
