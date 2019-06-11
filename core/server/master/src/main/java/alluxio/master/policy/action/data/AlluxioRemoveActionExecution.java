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
import alluxio.grpc.FreePOptions;
import alluxio.master.policy.action.ActionExecutionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The action to free a file from Alluxio.
 */
@ThreadSafe
public final class AlluxioRemoveActionExecution extends RemoveActionExecution {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioRemoveActionExecution.class);
  private static final FreePOptions FREE_OPTIONS = FreePOptions.newBuilder().setRecursive(true)
      .build();

  private final ActionExecutionContext mContext;
  private final String mPath;

  /**
   * @param ctx the context
   * @param path the Alluxio path to free data from
   */
  public AlluxioRemoveActionExecution(ActionExecutionContext ctx, String path) {
    mContext = ctx;
    mPath = path;
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected void remove() throws Exception {
    mContext.getFileSystem().free(new AlluxioURI(mPath), FREE_OPTIONS);
  }

  @Override
  public String toString() {
    return "ALLUXIO:REMOVE on path "  + mPath;
  }
}
