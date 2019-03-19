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

package alluxio.security.authentication.kerberos;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.ImpersonationAuthenticator;
import alluxio.security.authentication.SaslServerHandler;
import alluxio.security.util.KerberosName;
import alluxio.security.util.KerberosUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import java.io.IOException;

/**
 * CallbackHandler for SASL GSSAPI Kerberos mechanism.
 */
public class SaslServerCallbackHandlerKerberos implements CallbackHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(SaslServerCallbackHandlerKerberos.class);
  private final ImpersonationAuthenticator mImpersonationAuthenticator;
  private final SaslServerHandler mHandler;
  private AlluxioConfiguration mConfiguration;

  /**
   * Creates a new instance of {@link SaslServerCallbackHandlerKerberos}.
   *
   * @param handler {@link SaslServerHandler} instance that owns this callback
   * @param conf Alluxio configuration
   */
  public SaslServerCallbackHandlerKerberos(SaslServerHandler handler, AlluxioConfiguration conf) {
    mHandler = handler;
    mConfiguration = conf;
    mImpersonationAuthenticator = new ImpersonationAuthenticator(mConfiguration);
  }

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    AuthorizeCallback ac = null;
    for (Callback callback : callbacks) {
      if (callback instanceof AuthorizeCallback) {
        ac = (AuthorizeCallback) callback;
      } else {
        throw new UnsupportedCallbackException(callback, "Unrecognized SASL GSSAPI Callback");
      }
    }

    String authTranslator = mConfiguration.get(PropertyKey.SECURITY_KERBEROS_AUTH_TO_LOCAL);

    if (ac != null) {
      // Extract and verify the Kerberos id, which is the full principal name.
      String authenticationId = ac.getAuthenticationID();
      String authorizationId = ac.getAuthorizationID();

      try {
        authorizationId = new KerberosName(authorizationId).getShortName(authTranslator);
      } catch (Exception e) {
        // Ignore, since the impersonation user is not guaranteed to be a Kerberos name
      }

      String connectionUser;
      try {
        connectionUser = new KerberosName(authenticationId).getShortName(authTranslator);
        mImpersonationAuthenticator.authenticate(connectionUser, authorizationId);
        ac.setAuthorized(true);
      } catch (Exception e) {
        // Logging here to show the error on the master. Otherwise, error messages get swallowed.
        LOG.error("Impersonation failed.", e);
        ac.setAuthorized(false);
        throw e;
      }

      if (ac.isAuthorized()) {
        ac.setAuthorizedID(authorizationId);
        done(new KerberosName(authorizationId).getShortName(authTranslator), connectionUser);
      }
      // Do not set the AuthenticatedClientUser if the user is not authorized.
    }
  }

  /**
   * The done callback runs after the connection is successfully built.
   *
   * @param user the user
   * @param connectionUser
   */
  protected void done(String user, String connectionUser) {
    mHandler.setAuthenticatedUserInfo(
        new AuthenticatedUserInfo(user, connectionUser, KerberosUtils.GSSAPI_MECHANISM_NAME));
  }
}
