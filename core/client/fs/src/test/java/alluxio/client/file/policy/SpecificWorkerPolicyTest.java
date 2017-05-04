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

package alluxio.client.file.policy;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link SpecificWorkerPolicy}.
 */
public final class SpecificWorkerPolicyTest {

  /**
   * Tests that the specified worker is chosen.
   */
  @Test
  public void getWorkerTest() {
    List<BlockWorkerInfo> workerInfoList = Lists.newArrayList();
    WorkerNetAddress address =
        new WorkerNetAddress().setHost("worker1").setRpcPort(1).setDataPort(1).setWebPort(1);
    workerInfoList.add(new BlockWorkerInfo(address, Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(
        new WorkerNetAddress().setHost("worker1").setRpcPort(2).setDataPort(2).setWebPort(2),
        Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(
        new WorkerNetAddress().setHost("worker2").setRpcPort(1).setDataPort(1).setWebPort(1),
        Constants.GB, 0));
    SpecificWorkerPolicy policy = new SpecificWorkerPolicy(address);

    Assert.assertEquals(address, policy.getWorkerForNextBlock(workerInfoList, Constants.GB));
  }

}
