<<<<<<< HEAD
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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.master.file.meta.Inode;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A noop version of the {@link UfsDeleter}.
 */
@ThreadSafe
public final class NoopUfsDeleter implements UfsDeleter {
  public static final NoopUfsDeleter INSTANCE = new NoopUfsDeleter();

  @Override
  public boolean delete(AlluxioURI alluxioUri, Inode inode) {
    return true;
  }
}
||||||| merged common ancestors
=======
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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.master.file.meta.Inode;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A noop version of the {@link UfsDeleter}.
 */
@ThreadSafe
public final class NoopUfsDeleter implements UfsDeleter {
  public static final NoopUfsDeleter INSTANCE = new NoopUfsDeleter();

  /**
   * Constructs an instance of {@link NoopUfsDeleter}.
   */
  public NoopUfsDeleter() {}

  @Override
  public boolean delete(AlluxioURI alluxioUri, Inode inode) {
    return true;
  }
}
>>>>>>> origin/enterprise-1.5
