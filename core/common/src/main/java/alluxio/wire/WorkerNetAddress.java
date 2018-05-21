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

package alluxio.wire;

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.wire.TieredIdentity.LocalityTier;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The network address of a worker.
 */
@PublicApi
@NotThreadSafe
public final class WorkerNetAddress implements Serializable {
  private static final long serialVersionUID = 5822347646342091434L;

  // ALLUXIO CS ADD
  private int mSecureRpcPort;
  // ALLUXIO CS END
  private String mHost = "";
  private int mRpcPort;
  private int mDataPort;
  private int mWebPort;
  private String mDomainSocketPath = "";
  private TieredIdentity mTieredIdentity;

  /**
   * Creates a new instance of {@link WorkerNetAddress}.
   */
  public WorkerNetAddress() {}
  // ALLUXIO CS ADD

  /**
   * @return the secure rpc port
   */
  public int getSecureRpcPort() {
    return mSecureRpcPort;
  }
  // ALLUXIO CS END

  /**
   * @return the host of the worker
   */
  public String getHost() {
    return mHost;
  }

  /**
   * @return the RPC port
   */
  public int getRpcPort() {
    return mRpcPort;
  }

  /**
   * @return the data port
   */
  public int getDataPort() {
    return mDataPort;
  }

  /**
   * @return the web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * @return the domain socket path
   */
  public String getDomainSocketPath() {
    return mDomainSocketPath;
  }

  /**
   * @return the tiered identity
   */
  public TieredIdentity getTieredIdentity() {
    if (mTieredIdentity != null) {
      return mTieredIdentity;
    }
    return new TieredIdentity(Arrays.asList(new LocalityTier(Constants.LOCALITY_NODE, mHost)));
  }

  /**
   * @param host the host to use
   * @return the worker net address
   */
  public WorkerNetAddress setHost(String host) {
    Preconditions.checkNotNull(host, "host");
    mHost = host;
    return this;
  }

  /**
   * @param rpcPort the rpc port to use
   * @return the worker net address
   */
  public WorkerNetAddress setRpcPort(int rpcPort) {
    mRpcPort = rpcPort;
    return this;
  }

  /**
   * @param dataPort the data port to use
   * @return the worker net address
   */
  public WorkerNetAddress setDataPort(int dataPort) {
    mDataPort = dataPort;
    return this;
  }

  /**
   * @param webPort the web port to use
   * @return the worker net address
   */
  public WorkerNetAddress setWebPort(int webPort) {
    mWebPort = webPort;
    return this;
  }
  // ALLUXIO CS ADD

  /**
   * @param secureRpcPort the secure rpc port port to use
   * @return the worker net address
   */
  public WorkerNetAddress setSecureRpcPort(int secureRpcPort) {
    mSecureRpcPort = secureRpcPort;
    return this;
  }
  // ALLUXIO CS END

  /**
   * @param domainSocketPath the domain socket path
   * @return the worker net address
   */
  public WorkerNetAddress setDomainSocketPath(String domainSocketPath) {
    mDomainSocketPath = domainSocketPath;
    return this;
  }

  /**
   * @param tieredIdentity the tiered identity
   * @return the worker net address
   */
  public WorkerNetAddress setTieredIdentity(TieredIdentity tieredIdentity) {
    mTieredIdentity = tieredIdentity;
    return this;
  }

  /**
   * @return a net address of thrift construct
   */
  public alluxio.thrift.WorkerNetAddress toThrift() {
    alluxio.thrift.WorkerNetAddress address = new alluxio.thrift.WorkerNetAddress();
    // ALLUXIO CS ADD
    address.setSecureRpcPort(mSecureRpcPort);
    // ALLUXIO CS END
    address.setHost(mHost);
    address.setRpcPort(mRpcPort);
    address.setDataPort(mDataPort);
    address.setWebPort(mWebPort);
    address.setDomainSocketPath(mDomainSocketPath);
    if (mTieredIdentity != null) {
      address.setTieredIdentity(mTieredIdentity.toThrift());
    }
    return address;
  }

  /**
   * Creates a new instance of {@link WorkerNetAddress} from thrift representation.
   *
   * @param address the thrift net address
   * @return the instance
   */
  public static WorkerNetAddress fromThrift(alluxio.thrift.WorkerNetAddress address) {
    TieredIdentity tieredIdentity = TieredIdentity.fromThrift(address.getTieredIdentity());
    if (tieredIdentity == null) {
      // This means the worker is pre-1.7.0. We handle this in post-1.7.0 clients by filling out
      // the tiered identity using the hostname field.
      tieredIdentity = new TieredIdentity(
          Arrays.asList(new LocalityTier(Constants.LOCALITY_NODE, address.getHost())));
    }
    return new WorkerNetAddress()
        // ALLUXIO CS ADD
        .setSecureRpcPort(address.getSecureRpcPort())
        // ALLUXIO CS END
        .setDataPort(address.getDataPort())
        .setDomainSocketPath(address.getDomainSocketPath())
        .setHost(address.getHost())
        .setRpcPort(address.getRpcPort())
        .setTieredIdentity(tieredIdentity)
        .setWebPort(address.getWebPort());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkerNetAddress)) {
      return false;
    }
    WorkerNetAddress that = (WorkerNetAddress) o;
    return mHost.equals(that.mHost)
        // ALLUXIO CS ADD
        && mSecureRpcPort == that.mSecureRpcPort
        // ALLUXIO CS END
        && mRpcPort == that.mRpcPort
        && mDataPort == that.mDataPort
        && mWebPort == that.mWebPort
        && mDomainSocketPath.equals(that.mDomainSocketPath)
        && Objects.equal(mTieredIdentity, that.mTieredIdentity);
  }

  @Override
  public int hashCode() {
    // ALLUXIO CS REPLACE
    // return Objects.hashCode(mHost, mDataPort, mRpcPort, mWebPort, mDomainSocketPath,
    //     mTieredIdentity);
    // ALLUXIO CS WITH
    return Objects.hashCode(mSecureRpcPort, mHost, mDataPort, mRpcPort, mWebPort, mDomainSocketPath,
        mTieredIdentity);
    // ALLUXIO CS END
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        // ALLUXIO CS ADD
        .add("secureRpcPort", mSecureRpcPort)
        // ALLUXIO CS END
        .add("host", mHost)
        .add("rpcPort", mRpcPort)
        .add("dataPort", mDataPort)
        .add("webPort", mWebPort)
        .add("domainSocketPath", mDomainSocketPath)
        .add("tieredIdentity", mTieredIdentity)
        .toString();
  }
}
