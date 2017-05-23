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

package alluxio.underfs.fork;

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
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (IOException e : mExceptions) {
      sb.append("Exception #").append(++i).append(":\n");
      sb.append(e.toString());
    }
    return sb.toString();
  }
}
