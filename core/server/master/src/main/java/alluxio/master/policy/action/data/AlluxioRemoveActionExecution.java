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

import alluxio.AlluxioURI;
import alluxio.master.policy.action.ActionExecutionContext;
import alluxio.master.policy.action.CommitActionExecution;
import alluxio.master.policy.meta.InodeState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The action to free a file from Alluxio.
 */
@ThreadSafe
public final class AlluxioRemoveActionExecution extends CommitActionExecution {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioRemoveActionExecution.class);

  /**
   * @param ctx the execution context
   * @param path the Alluxio path
   * @param inode the inode
   */
  public AlluxioRemoveActionExecution(ActionExecutionContext ctx, String path,
      InodeState inode) {
    super(ctx, path, inode);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected void doCommit() throws Exception {
    mContext.getFileSystem().free(new AlluxioURI(mPath));
  }

  @Override
  public String getDescription() {
    return "ALLUXIO:REMOVE";
  }
}
