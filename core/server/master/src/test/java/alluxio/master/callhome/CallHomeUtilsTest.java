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

package alluxio.master.callhome;

import static org.junit.Assert.assertFalse;

import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link CallHomeUtils}.
 */
public final class CallHomeUtilsTest {

  @Test
  public void obfuscateDiagnostics() throws Exception {
    CallHomeInfo info = getTestDiagnosticInfo();
    info = CallHomeUtils.obfuscateDiagnostics(info);

    assertFalse(info.toString().contains("masterhost"));
    assertFalse(info.toString().contains("workerhost"));
  }

  CallHomeInfo getTestDiagnosticInfo() {
    CallHomeInfo info = new CallHomeInfo();
    info.setLicenseKey("dummy");
    info.setFaultTolerant(false);
    info.setStartTime(1111);
    info.setUptime(11111);
    info.setClusterVersion(RuntimeConstants.VERSION);
    // Set worker info
    info.setWorkerCount(4);
    info.setWorkerInfos(getTestWorkerInfoList("live", 3));
    info.setLostWorkerCount(1);
    info.setWorkerInfos(getTestWorkerInfoList("lost", 1));
    // Set ufs information.
    info.setUfsType("local");
    info.setUfsSize(11111);
    // Set storage tiers.
    List<CallHomeInfo.StorageTier> tiers = Lists.newArrayList();
    CallHomeInfo.StorageTier tier = new CallHomeInfo.StorageTier();
    tier.setAlias("MEM");
    tier.setSize(100);
    tier.setUsedSizeInBytes(50);
    tiers.add(tier);
    info.setStorageTiers(tiers.toArray(new CallHomeInfo.StorageTier[tiers.size()]));
    // Set file system master info
    info.setMasterAddress("masterhost/12.0.0.1:19998");
    info.setNumberOfPaths(100);
    return info;
  }

  WorkerInfo[] getTestWorkerInfoList(String prefix, int count) {
    WorkerInfo[] workerInfos = new WorkerInfo[count];
    for (int i = 0; i < count; ++i) {
      workerInfos[i] = new WorkerInfo();
      WorkerNetAddress address = new WorkerNetAddress();
      String host = prefix + "workerhost" + i;
      address.setHost(host);
      address.setTieredIdentity(new TieredIdentity(
          Arrays.asList(new TieredIdentity.LocalityTier(Constants.LOCALITY_NODE, host))));
      workerInfos[i].setAddress(address);
    }
    return workerInfos;
  }
}
