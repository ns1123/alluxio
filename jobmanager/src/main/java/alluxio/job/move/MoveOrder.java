/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job.move;

import java.io.Serializable;

/**
 * An order telling a worker to move a file.
 */
public final class MoveOrder implements Serializable {
  private static final long serialVersionUID = -4287491133291080690L;

  private final String mSrc;
  private final String mDst;

  /**
   * @param src the source file to move
   * @param dst the destination file to move it to
   */
  public MoveOrder(String src, String dst) {
    mSrc = src;
    mDst = dst;
  }

  /**
   * @return the dst
   */
  public String getDst() {
    return mDst;
  }
  /**
   * @return the src
   */
  public String getSrc() {
    return mSrc;
  }
}
