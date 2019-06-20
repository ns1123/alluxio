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

package alluxio.master.policy.meta;

import java.util.Map;

import javax.annotation.Nullable;

/**
 * A condition represents an expression which evaluates to a boolean.
 */
public interface InodeState {
  /**
   * @return the create time, in milliseconds
   */
  long getCreationTimeMs();

  /**
   * @return the id of the inode
   */
  long getId();

  /**
   * @return the last modification time, in milliseconds
   */
  long getLastModificationTimeMs();

  /**
   * @return the name of the inode
   */
  String getName();

  /**
   * @return any extended attributes on the inode
   */
  @Nullable
  Map<String, byte[]> getXAttr();

  /**
   * @return true if the inode is a directory, false otherwise
   */
  boolean isDirectory();

  /**
   * @return true if the inode is a file, false otherwise
   */
  boolean isFile();

  /**
   * @return true if the file has persisted, false otherwise
   */
  boolean isPersisted();

  /**
   * @return whether the inode is a mount point
   */
  boolean isMountPoint();
}
