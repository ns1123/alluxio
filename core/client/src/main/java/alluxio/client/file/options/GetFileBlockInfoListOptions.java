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

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Method options for getting a list of file block descriptors.
 */
@PublicApi
@ThreadSafe
public final class GetFileBlockInfoListOptions {
  /**
   * @return the default {@link GetFileBlockInfoListOptions}
   */
  @SuppressFBWarnings("ISC_INSTANTIATE_STATIC_CLASS")
  public static GetFileBlockInfoListOptions defaults() {
    return new GetFileBlockInfoListOptions();
  }

  private GetFileBlockInfoListOptions() {
    // No options currently
  }
}
