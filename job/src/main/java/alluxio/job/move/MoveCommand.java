/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.move;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * A command telling a worker to move a file.
 */
public final class MoveCommand implements Serializable {
  private static final long serialVersionUID = -4287491133291080690L;

  private final String mSource;
  private final String mDestination;

  /**
   * @param source the source file to move
   * @param destination the destination file to move it to
   */
  public MoveCommand(String source, String destination) {
    mSource = source;
    mDestination = destination;
  }

  /**
   * @return the source
   */
  public String getSource() {
    return mSource;
  }

  /**
   * @return the destination
   */
  public String getDestination() {
    return mDestination;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MoveCommand)) {
      return false;
    }
    MoveCommand that = (MoveCommand) o;
    return Objects.equal(mSource, that.mSource)
        && Objects.equal(mDestination, that.mDestination);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSource, mDestination);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("source", mSource)
        .add("destination", mDestination)
        .toString();
  }
}
