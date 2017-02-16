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
import alluxio.netty.NettyAttributes;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.channel.Channel;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;

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
   * Gets the Kerberos service name from {@link PropertyKey#SECURITY_KERBEROS_SERVICE_NAME}.
   *
   * @return the Kerberos service name
   * @throws IOException if the configuration is not set or empty
   */
  public static String getKerberosServiceName() throws IOException {
    if (Configuration.containsKey(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME)) {
      String serviceName = Configuration.get(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME);
      if (!serviceName.isEmpty()) {
        return serviceName;
      }
    }
    throw new IOException(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME.toString() + " must be set.");
  }

  /**
   * Extracts the {@link KerberosName} in the given subject.
   *
   * @param subject the given subject containing the login credentials
   * @return the extracted object
   */
  public static KerberosName extractKerberosNameFromSubject(Subject subject) {
    if (Boolean.getBoolean("sun.security.jgss.native")) {
      // Use MIT native Kerberos library
      Object[] principals = subject.getPrincipals().toArray();
      if (principals.length == 0 && CommonUtils.isAlluxioServer()) {
        // Server is ACCEPT_ONLY, the principal is null, need to fetch from configuration.
        String principal = Configuration.get(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL);
        if (principal.isEmpty()) {
          return null;
        }
        return new KerberosName(principal);
      }
      Principal clientPrincipal = (Principal) principals[0];
      return new KerberosName(clientPrincipal.getName());
    } else {
      Set<KerberosPrincipal> krb5Principals = subject.getPrincipals(KerberosPrincipal.class);
      if (!krb5Principals.isEmpty()) {
        // TODO(chaomin): for now at most one user is supported in one subject. Consider support
        // multiple Kerberos login users in the future.
        return new KerberosName(krb5Principals.iterator().next().toString());
      } else {
        return null;
      }
    }
  }

  /**
   * CallbackHandler for SASL GSSAPI Kerberos mechanism.
   */
  private abstract static class AbstractGssSaslCallbackHandler implements CallbackHandler {
    /**
     * Creates a new instance of {@link AbstractGssSaslCallbackHandler}.
     */
    public AbstractGssSaslCallbackHandler() {}

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
          done(new KerberosName(authorizationId).getServiceName());
        }
        // Do not set the AuthenticatedClientUser if the user is not authorized.
      }
    }

    /**
     * The done callback runs after the connection is successfully built.
     *
     * @param user the user
     */
    protected abstract void done(String user);
  }

  /**
   * The kerberos sasl callback for the thrift servers.
   */
  public static final class ThriftGssSaslCallbackHandler extends AbstractGssSaslCallbackHandler {
    private final Runnable mCallback;

    /**
     * Creates a {@link ThriftGssSaslCallbackHandler} instance.
     *
     * @param callback the callback runs after the connection is authenticated
     */
    public ThriftGssSaslCallbackHandler(Runnable callback) {
      mCallback = callback;
    }

    @Override
    protected void done(String user) {
      // After verification succeeds, a user with this authorizationId will be set to a
      // Threadlocal.
      try {
        User oldUser = AuthenticatedClientUser.get();
        Preconditions
            .checkState(oldUser == null, "A user (%s) exists while adding user (%s).", oldUser,
                user);
      } catch (IOException e) {
        // This should never happen.
        throw Throwables.propagate(e);
      }

      AuthenticatedClientUser.set(user);
      mCallback.run();
    }
  }

  /**
   * The kerberos sasl callback for the netty servers.
   */
  public static final class NettyGssSaslCallbackHandler extends AbstractGssSaslCallbackHandler {
    private Channel mChannel;

    /**
     * Creates an {@link NettyGssSaslCallbackHandler} instance.
     *
     * @param channel the netty channel
     */
    public NettyGssSaslCallbackHandler(Channel channel) {
      mChannel = channel;
    }

    @Override
    protected void done(String user) {
      mChannel.attr(NettyAttributes.CHANNEL_KERBEROS_USER_KEY).set(user);
    }
  }
}
