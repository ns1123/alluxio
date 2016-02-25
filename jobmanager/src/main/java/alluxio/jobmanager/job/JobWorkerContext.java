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

package alluxio.jobmanager.job;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.worker.block.BlockWorker;

/**
 * The context of worker-side resources.
 */
@ThreadSafe
public final class JobWorkerContext {
  private final FileSystem mFileSystem;
  private final BlockWorker mBlockWorker;

  /**
   * @param blockWorker the block worker
   */
  public JobWorkerContext(BlockWorker blockWorker) {
    mFileSystem = BaseFileSystem.get();
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
  }

  /**
   * @return the file system client
   */
  public FileSystem getFileSystem() {
    return mFileSystem;
  }

  /**
   * @return the block worker
   */
  public BlockWorker getBlockWorker() {
    return mBlockWorker;
  }
}
