/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a collection of exceptions.
 */
public class AggregateException extends IOException {
  private final Collection<IOException> mExceptions;

  /**
   * Creates a new instance of {@link AggregateException}.
   *
   * @param exceptions the nested exceptions
   */
  public AggregateException(Collection<IOException> exceptions) {
    mExceptions = exceptions;
  }

  @Override
  public String getMessage() {
    return toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (IOException e : mExceptions) {
      sb.append("Exception #").append(++i).append(":\n");
      sb.append(e.toString()).append("\n");
    }
    return sb.toString();
  }
}
