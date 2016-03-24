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

import alluxio.client.WriteType;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * A command telling a worker to move a file.
 */
public final class MoveCommand implements Serializable {
  private static final long serialVersionUID = -4287491133291080690L;

  private final String mSrc;
  private final String mDst;
  private final WriteType mWriteType;

  /**
   * @param src the source file to move
   * @param dst the destination file to move it to
   * @param writeType the write type to use when moving the file
   */
  public MoveCommand(String src, String dst, WriteType writeType) {
    mSrc = src;
    mDst = dst;
    mWriteType = writeType;
  }

  /**
   * @return the src
   */
  public String getSrc() {
    return mSrc;
  }

  /**
   * @return the dst
   */
  public String getDst() {
    return mDst;
  }

  /**
   * @return the writeType
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof MoveCommand)) {
      return false;
    }
    MoveCommand that = (MoveCommand) o;
    return Objects.equal(mSrc, that.mSrc)
        && Objects.equal(mDst, that.mDst)
        && Objects.equal(mWriteType, that.mWriteType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSrc, mDst, mWriteType);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("src", mSrc)
        .add("dst", mDst)
        .add("writeType", mWriteType)
        .toString();
  }
}
