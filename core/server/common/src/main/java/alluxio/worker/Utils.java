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

package alluxio.worker;

import alluxio.underfs.UfsManager;
import alluxio.util.io.PathUtils;

public class Utils {
  /** Magic number to write UFS block to UFS. */
  private static final String MAGIC_NUMBER = "1D91AC0E";

  /**
   * For a given block ID, derives the corresponding UFS file of this block if it falls back to
   * be stored in UFS.
   *
   * @param ufsInfo the UFS information for the mount point backing this file
   * @param blockId block ID
   * @return the UFS path of a block
   */
  public static String getUfsBlockPath(UfsManager.UfsInfo ufsInfo, long blockId) {
    return PathUtils.concatPath(ufsInfo.getUfsMountPointUri(),
        String.format(".alluxio_blocks_%s/%s/", MAGIC_NUMBER, blockId));
  }


  private Utils() {}  // prevent instantiation
}
