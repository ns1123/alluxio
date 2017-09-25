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

package alluxio.master.journal.raft;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.journal.JournalUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for the Raft journal system.
 */
public class RaftJournalConfiguration {
  private File mPath;
  private long mQuietTimeMs;
  private List<InetSocketAddress> mClusterAddresses;
  private InetSocketAddress mLocalAddress;
  private long mMaxLogSize;

  /**
   * @param serviceType either master raft service or job master raft service
   * @return default configuration for the specified service type
   */
  public static RaftJournalConfiguration defaults(ServiceType serviceType) {
    return new RaftJournalConfiguration()
        .setPath(new File(JournalUtils.getJournalLocation().getPath()))
        .setQuietTimeMs(
            Configuration.getMs(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS))
        .setClusterAddresses(defaultClusterAddresses(serviceType))
        .setLocalAddress(NetworkAddressUtils.getConnectAddress(serviceType))
        .setMaxLogSize(Configuration.getBytes(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX));
  }

  /**
   * @return where to store journal logs
   */
  public File getPath() {
    return mPath;
  }

  /**
   * @return when gaining primacy, wait for this duration to elapse without seeing new journal
   *         updates
   */
  public long getQuietTimeMs() {
    return mQuietTimeMs;
  }

  /**
   * @return addresses of all nodes in the Raft cluster
   */
  public List<InetSocketAddress> getClusterAddresses() {
    return mClusterAddresses;
  }

  /**
   * @return address of this Raft cluster node
   */
  public InetSocketAddress getLocalAddress() {
    return mLocalAddress;
  }

  /**
   * @return max log file size
   */
  public long getMaxLogSize() {
    return mMaxLogSize;
  }

  /**
   * @param path where to store journal logs
   * @return the updated configuration
   */
  public RaftJournalConfiguration setPath(File path) {
    mPath = path;
    return this;
  }

  /**
   * @param quietTimeMs when gaining primacy, wait for this duration to elapse without seeing new
   *        journal updates
   * @return the updated configuration
   */
  public RaftJournalConfiguration setQuietTimeMs(long quietTimeMs) {
    mQuietTimeMs = quietTimeMs;
    return this;
  }

  /**
   * @param clusterAddresses addresses of all nodes in the Raft cluster
   * @return the updated configuration
   */
  public RaftJournalConfiguration setClusterAddresses(List<InetSocketAddress> clusterAddresses) {
    mClusterAddresses = clusterAddresses;
    return this;
  }

  /**
   * @param localAddress address of this Raft cluster node
   * @return the updated configuration
   */
  public RaftJournalConfiguration setLocalAddress(InetSocketAddress localAddress) {
    mLocalAddress = localAddress;
    return this;
  }

  /**
   * @param maxLogSize maximum log file size
   * @return the updated configuration
   */
  public RaftJournalConfiguration setMaxLogSize(long maxLogSize) {
    mMaxLogSize = maxLogSize;
    return this;
  }

  private static List<InetSocketAddress> defaultClusterAddresses(ServiceType serviceType) {
    PropertyKey addressKey;
    if (serviceType.equals(ServiceType.MASTER_RAFT)) {
      addressKey = PropertyKey.MASTER_JOURNAL_RAFT_ADDRESSES;
    } else {
      Preconditions.checkState(serviceType.equals(ServiceType.JOB_MASTER_RAFT));
      addressKey = PropertyKey.JOB_MASTER_JOURNAL_RAFT_ADDRESSES;
    }
    List<String> addresses = Configuration.getList(addressKey, ",");
    List<InetSocketAddress> inetAddresses = new ArrayList<>();
    for (String address : addresses) {
      try {
        inetAddresses.add(NetworkAddressUtils.parseInetSocketAddress(address));
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Failed to parse address %s for property %s", address, addressKey), e);
      }
    }
    return inetAddresses;
  }
}
