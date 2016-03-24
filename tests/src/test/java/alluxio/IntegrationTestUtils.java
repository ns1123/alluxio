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

package alluxio;

import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;

import java.io.IOException;

/**
 * Util methods for writing integration tests.
 */
public final class IntegrationTestUtils {

  /**
   * Convenience method for calling {@link #waitForPersist(LocalAlluxioClusterResource, long, long)}
   * with a timeout of 5 seconds.
   *
   * @param localAlluxioClusterResource the cluster for the worker that will persist the file
   * @param fileId the file id to wait to be persisted
   * @throws AlluxioException
   * @throws IOException
   */
  public static void waitForPersist(LocalAlluxioClusterResource localAlluxioClusterResource,
      AlluxioURI uri) throws IOException, AlluxioException {
    waitForPersist(localAlluxioClusterResource, uri, 5 * Constants.SECOND_MS);
  }

  /**
   * Blocks until the specified file is persisted or a timeout occurs.
   *
   * @param localAlluxioClusterResource the cluster for the worker that will persist the file
   * @param fileId the file id to wait to be persisted
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   * @throws AlluxioException
   * @throws IOException
   */
  public static void waitForPersist(LocalAlluxioClusterResource localAlluxioClusterResource,
      AlluxioURI uri, int timeoutMs) throws IOException, AlluxioException {
    long start = System.currentTimeMillis();
    FileSystemMasterClient client =
        new FileSystemMasterClient(localAlluxioClusterResource.get().getMaster().getAddress(),
            localAlluxioClusterResource.getTestConf());
    while (!client.getStatus(uri).isPersisted()) {
      if (System.currentTimeMillis() - start > timeoutMs) {
        throw new RuntimeException("Timed out waiting for " + uri + " to be persisted");
      }
      CommonUtils.sleepMs(20);
    }
    client.close();
  }

  private IntegrationTestUtils() {} // This is a utils class not intended for instantiation
}
