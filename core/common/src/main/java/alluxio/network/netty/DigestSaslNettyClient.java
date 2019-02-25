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

package alluxio.network.netty;

import alluxio.conf.AlluxioConfiguration;
import alluxio.proto.security.CapabilityProto;
import alluxio.security.util.KerberosUtils;
import alluxio.util.proto.ProtoUtils;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * A Sasl secured Netty Client, with Digest Login.
 */
public class DigestSaslNettyClient {
  private static final Logger LOG = LoggerFactory.getLogger(DigestSaslNettyClient.class);

  private SaslClient mSaslClient;
  private CapabilityProto.Capability mCapability;

  /**
   * Constructs a DigestSaslNettyClient for authentication with servers using capability.
   *
   * @param capability the capability object for authentication
   * @param serverHostname the server hostname to authenticate with
   * @param conf Alluxio configuration
   * @throws SaslException if failed to create a Sasl netty client
   */
  public DigestSaslNettyClient(final CapabilityProto.Capability capability,
      final String serverHostname, AlluxioConfiguration conf) throws SaslException {
    Preconditions.checkNotNull(capability);
    mCapability = capability;

    // TODO(ggezer) Replace capability argument with an appropriate token class.
    // TODO(ggezer) Add Helpers for creating/parsing capability token content
    byte[] protoContent = ProtoUtils.getContent(mCapability);
    String strContentEnc = new String(Base64.encodeBase64(protoContent, false), Charsets.UTF_8);

    byte[] protoAuthenticator = ProtoUtils.getAuthenticator(mCapability);
    String strAuthenticatorEnc = new String(Base64.encodeBase64(protoAuthenticator, false), Charsets.UTF_8);

    final String serviceName = KerberosUtils.getKerberosServiceName(conf);
    final CallbackHandler ch =
        new KerberosUtils.SaslDigestClientCallbackHandler(strContentEnc, strAuthenticatorEnc);

    mSaslClient = Sasl.createSaslClient(new String[] {KerberosUtils.DIGEST_MECHANISM_NAME}, null,
        serviceName, serverHostname, KerberosUtils.SASL_PROPERTIES, ch);

    LOG.debug("Got Client: {}, Service: {}, Server Host :{}",
        mSaslClient, serviceName, serverHostname);
  }

  /**
   * Returns whether the Sasl client is complete.
   *
   * @return true iff the Sasl client is marked as complete, false otherwise
   */
  public boolean isComplete() {
    return mSaslClient.isComplete();
  }

  /**
   * Responds to server's Sasl token.
   *
   * @param token the server's Sasl token in byte array
   * @return client's response Sasl token
   * @throws SaslException if failed to respond to the given token
   */
  public byte[] response(final byte[] token) throws SaslException {
    return mSaslClient.evaluateChallenge(token);
  }

  /**
   *
   * @return the capability that is used for authentication
   */
  public CapabilityProto.Capability getCapability() {
    return mCapability;
  }
}
