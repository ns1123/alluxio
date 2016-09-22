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

package alluxio.security.util;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.security.authentication.AuthenticatedClientUser;

import java.security.AccessControlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

/**
 * Utils for Kerberos.
 */
public final class KerberosUtils {

  private KerberosUtils() {} // prevent instantiation

  public static final String GSSAPI_MECHANISM_NAME = "GSSAPI";

  /** Sasl properties. */
  public static final Map<String, String> SASL_PROPERTIES = Collections.unmodifiableMap(
      new HashMap<String, String>() {
        {
          put(Sasl.QOP, "auth");
        }
      }
  );

  /**
   * @return the Kerberos login module name
   */
  public static String getKrb5LoginModuleName() {
    return System.getProperty("java.vendor").contains("IBM")
        ? "com.ibm.security.auth.module.Krb5LoginModule"
        : "com.sun.security.auth.module.Krb5LoginModule";
  }

  /**
   * Parses a server Kerberos principal, which is stored in
   * {@link PropertyKey#SECURITY_KERBEROS_SERVER_PRINCIPAL}.
   *
   * @return a list of strings representing three parts: the primary, the instance, and the realm
   * @throws AccessControlException if server principal config is invalid
   * @throws SaslException if server principal config is not specified
   */
  public static KerberosName getServerKerberosName() throws AccessControlException, SaslException {
    String principal = Configuration.get(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL);
    if (principal.isEmpty()) {
      throw new SaslException("Failed to parse server principal: "
          + PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL.toString() + " must be set.");
    }
    return new KerberosName(principal);
  }

  /**
   * CallbackHandler for SASL GSSAPI Kerberos mechanism.
   */
  public static final class GssSaslCallbackHandler implements CallbackHandler {
    /**
     * Creates a new instance of {@link GssSaslCallbackHandler}.
     */
    public GssSaslCallbackHandler() {}

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL GSSAPI Callback");
        }
      }

      if (ac != null) {
        // Extract and verify the Kerberos id, which is the full principal name.
        // Currently because Kerberos impersonation is not supported, authenticationId and
        // authorizationId must match in order to make Kerberos login succeed.
        String authenticationId = ac.getAuthenticationID();
        String authorizationId = ac.getAuthorizationID();
        if (authenticationId.equals(authorizationId)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          ac.setAuthorizedID(authorizationId);
          // After verification succeeds, a user with this authorizationId will be set to a
          // Threadlocal.
          AuthenticatedClientUser.set(new KerberosName(authorizationId).getServiceName());
        }
        // Do not set the AuthenticatedClientUser if the user is not authorized.
      }
    }
  }
}
