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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterProcess;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.license.License;
import alluxio.master.license.LicenseMaster;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link CallHomeUtils}.
 */
public final class CallHomeUtilsTest {

  @Test
  public void collectDiagnostics() throws Exception {
    // Create mock classes
    MasterProcess mockMasterProcess = Mockito.mock(MasterProcess.class);
    when(mockMasterProcess.getStartTimeMs()).thenReturn(System.currentTimeMillis());
    String masterHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
    InetSocketAddress masterAddress = new InetSocketAddress(masterHostname, 19998);
    when(mockMasterProcess.getRpcAddress()).thenReturn(masterAddress);

    BlockMaster mockBlockMaster = Mockito.mock(BlockMaster.class);
    List<WorkerInfo> liveWorkers = Arrays.asList(getTestWorkerInfoList("live", 1));
    when(mockBlockMaster.getWorkerCount()).thenReturn(1);
    when(mockBlockMaster.getWorkerInfoList()).thenReturn(liveWorkers);
    List<WorkerInfo> lostWorkers = Arrays.asList(getTestWorkerInfoList("lost", 1));
    when(mockBlockMaster.getLostWorkerCount()).thenReturn(1);
    when(mockBlockMaster.getLostWorkersInfoList()).thenReturn(lostWorkers);
    when(mockBlockMaster.getGlobalStorageTierAssoc()).thenReturn(new MasterStorageTierAssoc());

    License mockLicense = Mockito.mock(License.class);
    when(mockLicense.getKey()).thenReturn("key");
    when(mockLicense.getToken()).thenReturn("token");

    LicenseMaster mockLicenseMaster = Mockito.mock(LicenseMaster.class);
    when(mockLicenseMaster.getLicense()).thenReturn(mockLicense);

    FileSystemMaster mockFsMaster = Mockito.mock(FileSystemMaster.class);

    // Check collect diagnostics makes a copy of worker infos
    CallHomeInfo info = CallHomeUtils.collectDiagnostics(mockMasterProcess, mockBlockMaster,
        mockLicenseMaster, mockFsMaster);
    info.setMasterAddress("overwritemaster");
    info.getWorkerInfos()[0].getAddress().setHost("overwritelive");
    info.getLostWorkerInfos()[0].getAddress().setHost("overwritelost");
    assertTrue(masterAddress.getHostString().equals(masterHostname));
    assertTrue(liveWorkers.get(0).getAddress().getHost().startsWith("live"));
    assertTrue(lostWorkers.get(0).getAddress().getHost().startsWith("lost"));
  }

  @Test
  public void obfuscateDiagnostics() throws Exception {
    CallHomeInfo info = getTestDiagnosticInfo();
    info = CallHomeUtils.obfuscateDiagnostics(info);

    assertFalse(info.toString().contains("masterhost"));
    assertFalse(info.toString().contains("workerhost"));
  }

  CallHomeInfo getTestDiagnosticInfo() {
    CallHomeInfo info = new CallHomeInfo();

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
    // Set license info
    CallHomeInfo.LicenseInfo licenseInfo = new CallHomeInfo.LicenseInfo();
    licenseInfo.setToken("token");
    licenseInfo.setLicenseKey("dummy");
    licenseInfo.setEmail("test@alluxio.com");
    info.setLicenseInfo(licenseInfo);
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
