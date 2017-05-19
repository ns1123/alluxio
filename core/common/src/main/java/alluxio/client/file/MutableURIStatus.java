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

package alluxio.client.file;

import alluxio.wire.FileInfo;

/**
 * A mutable {@link URIStatus}.
 */
public class MutableURIStatus extends URIStatus {
  /**
   * Creates a new {@link MutableURIStatus} from a {@link FileInfo}.
   *
   * @param fileInfo the file info
   */
  public MutableURIStatus(FileInfo fileInfo) {
    super(fileInfo);
  }

  /**
   * Creates a new {@link MutableURIStatus} from a {@link URIStatus}.
   *
   * @param status the file info
   */
  public MutableURIStatus(URIStatus status) {
    super(status.getFileInfo());
  }

  /**
   * @return the file info
   */
  public FileInfo getFileInfo() {
    return super.getFileInfo();
  }
}
