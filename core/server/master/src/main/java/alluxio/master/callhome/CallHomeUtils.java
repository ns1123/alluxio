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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.MasterProcess;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.license.License;
import alluxio.master.license.LicenseMaster;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * This class encapsulates utilities for collecting diagnostic info.
 */
public final class CallHomeUtils {
  private CallHomeUtils() {}  // prevent instantiation

  /**
   * @param masterProcess the Alluxio master process
   * @param blockMaster the block master
   * @param licenseMaster the license master
   * @param fsMaster the file system master
   * @return the collected call home information, null if license hasn't been loaded
   * @throws IOException when failed to collect call home information
   */
  @Nullable
  public static CallHomeInfo collectDiagnostics(MasterProcess masterProcess,
      BlockMaster blockMaster, LicenseMaster licenseMaster, FileSystemMaster fsMaster)
      throws IOException {
    License license = licenseMaster.getLicense();
    if (license.getKey() == null) {
      // License hasn't been loaded.
      return null;
    }
    CallHomeInfo info = new CallHomeInfo();
    info.setLicenseKey(license.getKey());
    info.setFaultTolerant(Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    info.setWorkerCount(blockMaster.getWorkerCount());
    List<WorkerInfo> workerInfos = blockMaster.getWorkerInfoList();
    info.setWorkerInfos(workerInfos.toArray(new WorkerInfo[workerInfos.size()]));
    info.setLostWorkerCount(blockMaster.getLostWorkerCount());
    List<WorkerInfo> lostWorkerInfos = blockMaster.getWorkerInfoList();
    info.setWorkerInfos(lostWorkerInfos.toArray(new WorkerInfo[lostWorkerInfos.size()]));
    info.setStartTime(masterProcess.getStartTimeMs());
    info.setUptime(masterProcess.getUptimeMs());
    info.setClusterVersion(RuntimeConstants.VERSION);
    // Set ufs information.
    String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot();
    info.setUfsType(ufs.getUnderFSType());
    info.setUfsSize(ufs.getSpace(ufsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL));
    // Set storage tiers.
    List<String> aliases = blockMaster.getGlobalStorageTierAssoc().getOrderedStorageAliases();
    java.util.Map<String, Long> tierSizes = blockMaster.getTotalBytesOnTiers();
    java.util.Map<String, Long> usedTierSizes = blockMaster.getUsedBytesOnTiers();
    List<CallHomeInfo.StorageTier> tiers = com.google.common.collect.Lists.newArrayList();
    for (String alias : aliases) {
      CallHomeInfo.StorageTier tier = new CallHomeInfo.StorageTier();
      if (tierSizes.containsKey(alias)) {
        tier.setAlias(alias);
        tier.setSize(tierSizes.get(alias));
        tier.setUsedSizeInBytes(usedTierSizes.get(alias));
        tiers.add(tier);
      }
    }
    info.setStorageTiers(tiers.toArray(new CallHomeInfo.StorageTier[tiers.size()]));
    // Set file system master info
    info.setMasterAddress(masterProcess.getRpcAddress().toString());
    info.setNumberOfPaths(fsMaster.getNumberOfPaths());
    return info;
  }

  /**
   * @param info the diagnostic info
   * @return obfuscated diagnostic info
   */
  public static CallHomeInfo obfuscateDiagnostics(CallHomeInfo info)
      throws GeneralSecurityException {
    // Master hostname
    info.setMasterAddress(obfuscateAddress(info.getMasterAddress()));

    // Worker addresses
    if (info.getWorkerInfos() != null) {
      info.setWorkerInfos(obfuscateWorkerInfos(info.getWorkerInfos()));
    }
    if (info.getLostWorkerInfos() != null) {
      info.setLostWorkerInfos(obfuscateWorkerInfos(info.getLostWorkerInfos()));
    }
    return info;
  }

  /**
   * Encode the entire string.
   */
  private static String obfuscateString(String input) throws GeneralSecurityException {
    if (input == null) {
      return input;
    }
    MessageDigest md = MessageDigest.getInstance("MD5");
    return new String(org.apache.commons.codec.binary.Hex.encodeHex(md.digest(input.getBytes())));
  }

  /**
   * Encode the host portion from an address string.
   */
  private static String obfuscateAddress(String address) throws GeneralSecurityException {
    int index = address.indexOf(":");
    if (index >= 0 && index < address.length()) {
      // Address has port
      StringBuilder builder = new StringBuilder();
      builder.append(obfuscateString(address.substring(0, index)));
      builder.append(":");
      // Do not obfuscate port
      builder.append(address.substring(index + 1, address.length()));
      return builder.toString();
    }
    return address;
  }

  /**
   * Encode the host from a worker's address string.
   */
  private static WorkerInfo[] obfuscateWorkerInfos(WorkerInfo[] workerInfos)
      throws GeneralSecurityException {
    if (workerInfos == null) {
      return workerInfos;
    }
    for (int i = 0; i < workerInfos.length; ++i) {
      WorkerNetAddress address = workerInfos[i].getAddress();
      if (address != null) {
        // Host address
        workerInfos[i].getAddress().setHost(obfuscateString(address.getHost()));

        // Tiered locality
        TieredIdentity identity = address.getTieredIdentity();
        List<TieredIdentity.LocalityTier> obfuscatedTiers = new ArrayList<>();
        for (TieredIdentity.LocalityTier tier : identity.getTiers()) {
          obfuscatedTiers.add(new TieredIdentity.LocalityTier(tier.getTierName(),
              obfuscateString(tier.getValue())));
        }
        address.setTieredIdentity(new TieredIdentity(obfuscatedTiers));
      }
    }
    return workerInfos;
  }
}
