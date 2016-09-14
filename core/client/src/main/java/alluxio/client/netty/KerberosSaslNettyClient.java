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

package alluxio.client.netty;

import alluxio.Constants;
import alluxio.security.LoginUser;
import alluxio.security.util.KerberosName;
import alluxio.security.util.KerberosUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * A Sasl secured Netty Client, with Kerberos Login.
 */
public class KerberosSaslNettyClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private SaslClient mSaslClient;
  private Subject mSubject;

  /**
   * Constructs a KerberosSaslNettyClient for authentication with servers.
   *
   * @throws SaslException if failed to create a Sasl netty client
   */
  public KerberosSaslNettyClient() throws SaslException {
    KerberosName name;
    try {
      name = KerberosUtils.getServerKerberosName();
    } catch (AccessControlException e) {
      throw new SaslException("AccessControlException: " + e);
    }

    try {
      mSubject = LoginUser.getClientLoginSubject();
    } catch (IOException e) {
      throw new SaslException("IOException: " + e);
    }

    try {
      final String hostName = name.getHostName();
      final String serviceName = name.getServiceName();
      final CallbackHandler ch = new SaslClientCallbackHandler();
      mSaslClient = Subject.doAs(mSubject, new PrivilegedExceptionAction<SaslClient>() {
        public SaslClient run() {
          try {
            return Sasl.createSaslClient(
                new String[] { KerberosUtils.GSSAPI_MECHANISM_NAME }, null /* authorizationId */,
                serviceName, hostName, KerberosUtils.SASL_PROPERTIES, ch);
          } catch (Exception e) {
            LOG.error("Subject failed to create Sasl client.", e);
            return null;
          }
        }
      });
      LOG.debug("Got Client: {}", mSaslClient);
    } catch (PrivilegedActionException e) {
      LOG.error("KerberosSaslNettyClient: Could not create Sasl Netty Client.");
      throw new SaslException("PrivilegedActionException: " + e);
    }
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
   * Respond to server's Sasl token.
   *
   * @param token the server's Sasl token in byte array
   * @return client's response Sasl token
   * @throws SaslException if failed to respond to the given token
   */
  public byte[] response(final byte[] token) throws SaslException {
    try {
      return Subject.doAs(mSubject, new PrivilegedExceptionAction<byte[]>() {
        public byte[] run() throws SaslException {
          LOG.debug("response: Responding to input token of length: {}", token.length);
          return mSaslClient.evaluateChallenge(token);
        }
      });
    } catch (PrivilegedActionException e) {
      LOG.error("Failed to generate response for token: ", e);
      throw new SaslException(e.getMessage());
    }
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler.
   */
  private static class SaslClientCallbackHandler implements CallbackHandler {
    /**
     * Creates a new instance of {@link SaslClientCallbackHandler}.
     */
    public SaslClientCallbackHandler() {}

    /**
     * Implementation used to respond to Sasl tokens from server.
     *
     * @param callbacks
     *            objects that indicate what credential information the
     *            server's SaslServer requires from the client.
     * @throws UnsupportedCallbackException
     */
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        LOG.info("Kerberos Client Callback Handler got callback: {}", callback.getClass());
      }
    }
  }
}
