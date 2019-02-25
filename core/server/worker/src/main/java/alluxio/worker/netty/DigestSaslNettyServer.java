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

package alluxio.worker.netty;

import alluxio.conf.ServerConfiguration;
import alluxio.security.MasterKey;
import alluxio.security.util.KerberosUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.List;

/**
 * A Sasl secured Netty Server, with Digest Login.
 */
public class DigestSaslNettyServer {
  private static final Logger LOG = LoggerFactory.getLogger(DigestSaslNettyServer.class);
  private Channel mChannel;
  private SaslServer mSaslServer;

  /**
   * Constructs a DigestSaslNettyServer.
   *
   * @param channel the channel
   * @param activeMasterKeys list of active capability master keys
   * @throws SaslException if failed to create a Sasl netty server
   */
  public DigestSaslNettyServer(final Channel channel,
      final List<MasterKey> activeMasterKeys) throws SaslException {
    mChannel = channel;

    final String hostname = NetworkAddressUtils
        .getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC, ServerConfiguration.global());
    final String serviceName = KerberosUtils.getKerberosServiceName(ServerConfiguration.global());
    Preconditions.checkNotNull(hostname);

    mSaslServer = Sasl.createSaslServer(KerberosUtils.DIGEST_MECHANISM_NAME, serviceName, hostname,
        KerberosUtils.SASL_PROPERTIES,
        new KerberosUtils.NettyDigestServerCallbackHandler(mChannel, activeMasterKeys));
  }

  /**
   * Returns whether the Sasl server is complete.
   *
   * @return true iff the Sasl server is marked as complete, false otherwise
   */
  public boolean isComplete() {
    return mSaslServer.isComplete();
  }

  /**
   * Generates the response to a SASL tokens.
   *
   * @param token Server's SASL token
   * @return token to send back to the the other side
   * @throws SaslException if failed to respond to the given token
   */
  public byte[] response(final byte[] token) throws SaslException {
    return mSaslServer.evaluateResponse(token);
  }
}

