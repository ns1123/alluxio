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

package alluxio.master.file.replication;

import alluxio.Constants;
import alluxio.exception.AlluxioException;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Interface to send requests to adjust block replication level, by either replicating or evicting
 * blocks.
 */
public interface AdjustReplicationHandler {
  /**
   * Factory for {@link AdjustReplicationHandler}.
   */
  @ThreadSafe
  class Factory {
    public static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

    private Factory() {} // prevent instantiation

    /**
     * Creates a new instance of {@link AdjustReplicationHandler}.
     *
     * @param className name of an implementation class of {@link AdjustReplicationHandler}
     * @return the generated {@link AdjustReplicationHandler}
     */
    public static AdjustReplicationHandler create(String className) {
      try {
        return (AdjustReplicationHandler) Class.forName(className).getConstructor().newInstance();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Adjust the block replication level by a target number of replicas (either replicate or evict).
   *
   * @param blockId ID of the block
   * @param numReplicas how many replicas to add or remove
   * @throws AlluxioException if the adjusting fails
   */
  void adjust(long blockId, int numReplicas) throws AlluxioException;
}
