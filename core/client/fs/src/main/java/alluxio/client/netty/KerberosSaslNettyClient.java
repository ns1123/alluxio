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

import alluxio.security.LoginUser;
import alluxio.security.util.KerberosUtils;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private static final Logger LOG = LoggerFactory.getLogger(KerberosSaslNettyClient.class);

  private SaslClient mSaslClient;
  private Subject mSubject;

  /**
   * Constructs a KerberosSaslNettyClient for authentication with servers.
   *
   * @param serverHostname the server hostname to authenticate with
   * @throws SaslException if failed to create a Sasl netty client
   */
  public KerberosSaslNettyClient(final String serverHostname) throws SaslException {
    try {
      mSubject = LoginUser.getClientLoginSubject();
    } catch (IOException e) {
      throw new SaslException("Failed to get client login subject", e);
    }

    final String serviceName = KerberosUtils.getKerberosServiceName();
    final CallbackHandler ch = new SaslClientCallbackHandler();
    try {
      mSaslClient = Subject.doAs(mSubject, new PrivilegedExceptionAction<SaslClient>() {
        public SaslClient run() throws SaslException {
            return Sasl.createSaslClient(
                new String[] { KerberosUtils.GSSAPI_MECHANISM_NAME }, null /* authorizationId */,
                serviceName, serverHostname, KerberosUtils.SASL_PROPERTIES, ch);
        }
      });
    } catch (PrivilegedActionException e) {
      LOG.error("Subject failed to create Sasl client. ", e);
      Throwables.propagateIfPossible(e.getCause(), SaslException.class);
      throw new RuntimeException(e.getCause());
    }
    LOG.debug("Got Client: {}", mSaslClient);
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
    try {
      return Subject.doAs(mSubject, new PrivilegedExceptionAction<byte[]>() {
        public byte[] run() throws SaslException {
          return mSaslClient.evaluateChallenge(token);
        }
      });
    } catch (PrivilegedActionException e) {
      throw new SaslException("Failed to generate response for token. ", e);
    }
  }

  /**
   * A simple client SASL callback handler.
   */
  private static class SaslClientCallbackHandler implements CallbackHandler {
    /**
     * Creates a new instance of {@link SaslClientCallbackHandler}.
     */
    public SaslClientCallbackHandler() {}

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        LOG.info("Kerberos Client Callback Handler got callback: {}", callback.getClass());
      }
    }
  }
}
