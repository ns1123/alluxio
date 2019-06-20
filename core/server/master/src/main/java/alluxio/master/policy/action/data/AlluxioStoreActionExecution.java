/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.policy.action.data;

import alluxio.job.JobConfig;
import alluxio.job.load.LoadConfig;
import alluxio.master.policy.action.ActionExecutionContext;
import alluxio.master.policy.action.JobServiceActionExecution;
import alluxio.master.policy.meta.InodeState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The action to load a file from UFS to Alluxio.
 */
@ThreadSafe
public final class AlluxioStoreActionExecution extends JobServiceActionExecution {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioStoreActionExecution.class);

  /**
   * @param ctx the execution context
   * @param path the Alluxio path
   * @param inode the inode
   */
  public AlluxioStoreActionExecution(ActionExecutionContext ctx, String path,
      InodeState inode) {
    super(ctx, path, inode);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected JobConfig createJobConfig() {
    return new LoadConfig(mPath, 1 /* replication */);
  }

  @Override
  public String getDescription() {
    return "ALLUXIO:STORE";
  }
}
