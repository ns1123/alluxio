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

package alluxio.master;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Client for determining the primary master.
 */
@ThreadSafe
public interface MasterInquireClient extends AutoCloseable {
  /**
   * @return the rpc address of the primary master. The implementation should perform retries if
   *         appropriate
   * @throws UnavailableException if the primary rpc address cannot be determined
   */
  InetSocketAddress getPrimaryRpcAddress() throws UnavailableException;

  /**
   * @return a list of all masters' RPC addresses
   * @throws UnavailableException if the master rpc addresses cannot be determined
   */
  List<InetSocketAddress> getMasterRpcAddresses() throws UnavailableException;

  /**
   * Factory for getting a master inquire client.
   */
  class Factory {
    /**
     * Creates an instance of {@link MasterInquireClient} based on the current configuration. The
     * returned instance may be shared, so it should not be closed by callers of this method.
     *
     * @return a master inquire client
     */
    public static MasterInquireClient create() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return ZkMasterInquireClient.getClient(Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS),
            Configuration.get(PropertyKey.ZOOKEEPER_ELECTION_PATH),
            Configuration.get(PropertyKey.ZOOKEEPER_LEADER_PATH));
        // ALLUXIO CS ADD
      } else if (getRpcAddresses().size() > 1) {
        return new PollingMasterInquireClient(getRpcAddresses());
        // ALLUXIO CS END
      } else {
        return new SingleMasterInquireClient(
            NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));
      }
    }

    // ALLUXIO CS ADD
    public static MasterInquireClient createForJobMaster() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return ZkMasterInquireClient.getClient(Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS),
            Configuration.get(PropertyKey.ZOOKEEPER_JOB_ELECTION_PATH),
            Configuration.get(PropertyKey.ZOOKEEPER_JOB_LEADER_PATH));
      } else if (getJobRpcAddresses().size() > 1) {
        return new PollingMasterInquireClient(getJobRpcAddresses());
      } else {
        return new SingleMasterInquireClient(
            NetworkAddressUtils.getConnectAddress(ServiceType.JOB_MASTER_RPC));
      }
    }

    /**
     * Gets the RPC addresses of all masters based on the configuration.
     *
     * If {@link PropertyKey#MASTER_RPC_ADDRESSES} is explicitly set, we parse and use those
     * addresses. Otherwise, we combine the hostnames in
     * {@link PropertyKey#MASTER_EMBEDDED_JOURNAL_ADDRESSES} with
     * {@link PropertyKey#MASTER_EMBEDDED_JOURNAL_PORT}
     *
     * @return the master rpc addresses
     */
    private static List<InetSocketAddress> getRpcAddresses() {
      if (Configuration.containsKey(PropertyKey.MASTER_RPC_ADDRESSES)) {
        List<String> rpcAddresses = Configuration.getList(PropertyKey.MASTER_RPC_ADDRESSES, ",");
        return getRpcInetSocketAddresses(rpcAddresses);
      } else {
        return getRpcAddressesFromRaftAddresses(
            NetworkAddressUtils.getPort(ServiceType.MASTER_RPC));
      }
    }

    /**
     * Gets the RPC addresses of all job masters based on the configuration.
     *
     * If {@link PropertyKey#JOB_MASTER_RPC_ADDRESSES} is explicitly set, we parse and use those
     * addresses. Otherwise, we combine the hostnames in
     * {@link PropertyKey#JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES} with
     * {@link PropertyKey#JOB_MASTER_EMBEDDED_JOURNAL_PORT}
     *
     * @return the job master rpc addresses
     */
    private static List<InetSocketAddress> getJobRpcAddresses() {
      if (Configuration.containsKey(PropertyKey.JOB_MASTER_RPC_ADDRESSES)) {
        List<String> rpcAddresses =
            Configuration.getList(PropertyKey.JOB_MASTER_RPC_ADDRESSES, ",");
        return getRpcInetSocketAddresses(rpcAddresses);
      } else {
        return getRpcAddressesFromRaftAddresses(
            NetworkAddressUtils.getPort(ServiceType.JOB_MASTER_RPC));
      }
    }

    /**
     * @param rpcAddresses a list of RPC address strings
     * @return a list of InetSocketAddresses representing the given address strings
     */
    private static List<InetSocketAddress> getRpcInetSocketAddresses(List<String> rpcAddresses) {
      List<InetSocketAddress> inetSocketAddresses = new java.util.ArrayList<>(rpcAddresses.size());
      for (String address : rpcAddresses) {
        try {
          inetSocketAddresses.add(NetworkAddressUtils.parseInetSocketAddress(address));
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed to parse host:port: " + address, e);
        }
      }
      return inetSocketAddresses;
    }

    /**
     * @param rpcPort an RPC port
     * @return a list of InetSocketAddresses combining the configured Raft hostnames with the given
     *         RPC port
     */
    private static List<InetSocketAddress> getRpcAddressesFromRaftAddresses(int rpcPort) {
      List<InetSocketAddress> inetSocketAddresses = new java.util.ArrayList<>();
      for (String address : Configuration.getList(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES,
          ",")) {
        try {
          String rpcHost = NetworkAddressUtils.parseInetSocketAddress(address).getHostName();
          inetSocketAddresses.add(new InetSocketAddress(rpcHost, rpcPort));
        } catch (IOException e) {
          throw new IllegalArgumentException("Failed to parse host:port: " + address, e);
        }
      }
      return inetSocketAddresses;
    }
    // ALLUXIO CS END
    private Factory() {} // Not intended for instantiation.
  }
}
