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

import static java.util.stream.Collectors.toList;

import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.MasterProcess;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.license.License;
import alluxio.master.license.LicenseMaster;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.net.NetworkInterface;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Enumeration;
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
          throws IOException, GeneralSecurityException {
    License license = licenseMaster.getLicense();
    if (license.getKey() == null) {
      // License hasn't been loaded.
      return null;
    }
    CallHomeInfo info = new CallHomeInfo();
    info.setProduct("enterprise");
    info.setFaultTolerant(ServerConfiguration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED));
    info.setWorkerCount(blockMaster.getWorkerCount());
    List<WorkerInfo> workerInfos = blockMaster.getWorkerInfoList();
    // Make a copy
    info.setWorkerInfos(workerInfos.stream().map(workerInfo -> SerializationUtils.clone(workerInfo))
        .collect(toList()).toArray(new WorkerInfo[workerInfos.size()]));
    info.setLostWorkerCount(blockMaster.getLostWorkerCount());
    List<WorkerInfo> lostWorkerInfos = blockMaster.getLostWorkersInfoList();
    // Make a copy
    info.setLostWorkerInfos(
        lostWorkerInfos.stream().map(lostWorkerInfo -> SerializationUtils.clone(lostWorkerInfo))
            .collect(toList()).toArray(new WorkerInfo[lostWorkerInfos.size()]));
    info.setStartTime(masterProcess.getStartTimeMs());
    info.setUptime(masterProcess.getUptimeMs());
    info.setClusterVersion(RuntimeConstants.VERSION);
    // Set ufs information.
    String ufsRoot = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
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
    info.setMACAddress(getMACAddress(masterProcess));

    CallHomeInfo.LicenseInfo licenseInfo = new CallHomeInfo.LicenseInfo();
    licenseInfo.setLicenseKey(license.getKey());
    licenseInfo.setEmail(license.getEmail());
    licenseInfo.setToken(license.getToken());
    info.setLicenseInfo(licenseInfo);

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

  /**
   * @return the MAC address of the network interface being used by the master
   * @throws IOException when no MAC address is found
   */
  private static String getMACAddress(MasterProcess masterProcess) throws IOException {
    // Try to get the MAC address of the network interface of the master's RPC address.
    NetworkInterface nic =
            NetworkInterface.getByInetAddress(masterProcess.getRpcAddress().getAddress());
    byte[] mac = nic.getHardwareAddress();
    if (mac != null) {
      return new String(mac);
    }

    // Try to get the MAC address of the common "en0" interface.
    nic = NetworkInterface.getByName("en0");
    mac = nic.getHardwareAddress();
    if (mac != null) {
      return new String(mac);
    }

    // Try to get the first non-empty MAC address in the enumeration of all network interfaces.
    Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
    while (ifaces.hasMoreElements()) {
      nic = ifaces.nextElement();
      if (nic == null) {
        continue;
      }
      mac = nic.getHardwareAddress();
      if (mac != null) {
        return new String(mac);
      }
    }

    throw new IOException("No MAC address was found");
  }
}
