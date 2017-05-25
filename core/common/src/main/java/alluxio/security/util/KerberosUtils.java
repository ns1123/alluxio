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
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;

/**
 * Utils for Kerberos.
 */
public final class KerberosUtils {
  public static final String GSSAPI_MECHANISM_NAME = "GSSAPI";
  // The constant below identifies the Kerberos v5 GSS-API mechanism type, see
  // https://docs.oracle.com/javase/7/docs/api/org/ietf/jgss/GSSManager.html for details
  public static final String GSSAPI_MECHANISM_ID = "1.2.840.113554.1.2.2";

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
   * @return the default Kerberos realm name
   * @throws ClassNotFoundException if class is not found to get Kerberos conf
   * @throws NoSuchMethodException if there is no such method to get Kerberos conf
   * @throws IllegalArgumentException if the argument for Kerberos conf is invalid
   * @throws IllegalAccessException if the underlying method is inaccessible
   * @throws InvocationTargetException if the underlying method throws such exception
   */
  public static String getDefaultRealm() throws ClassNotFoundException, NoSuchMethodException,
      IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    Object kerbConf;
    Class<?> classRef;
    Method getInstanceMethod;
    Method getDefaultRealmMethod;
    if (System.getProperty("java.vendor").contains("IBM")) {
      classRef = Class.forName("com.ibm.security.krb5.internal.Config");
    } else {
      classRef = Class.forName("sun.security.krb5.Config");
    }
    getInstanceMethod = classRef.getMethod("getInstance", new Class[0]);
    kerbConf = getInstanceMethod.invoke(classRef, new Object[0]);
    getDefaultRealmMethod = classRef.getDeclaredMethod("getDefaultRealm", new Class[0]);
    return (String) getDefaultRealmMethod.invoke(kerbConf, new Object[0]);
  }

  /**
   * Gets the Kerberos service name from {@link PropertyKey#SECURITY_KERBEROS_SERVICE_NAME}.
   *
   * @return the Kerberos service name
   */
  public static String getKerberosServiceName() {
    String serviceName = Configuration.get(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME);
    if (!serviceName.isEmpty()) {
      return serviceName;
    }
    throw new RuntimeException(
        PropertyKey.SECURITY_KERBEROS_SERVICE_NAME.toString() + " must be set.");
  }

  /**
   * @return the Kerberos unified instance name if set, otherwise null
   */
  public static String maybeGetKerberosUnifiedInstanceName() {
    if (!Configuration.containsKey(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME)) {
      return null;
    }
    String unifedInstance = Configuration.get(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME);
    if (unifedInstance.isEmpty()) {
      return null;
    }
    return unifedInstance;
  }

  /**
   * Gets the {@link GSSCredential} from JGSS.
   *
   * @return the credential
   * @throws GSSException if it failed to get the credential
   */
  public static GSSCredential getCredentialFromJGSS() throws GSSException {
    GSSManager gssManager = GSSManager.getInstance();
    Oid krb5Mechanism = new Oid(GSSAPI_MECHANISM_ID);

    // When performing operations as a particular Subject, the to-be-used GSSCredential
    // should be added to Subject's private credential set. Otherwise, the GSS operations
    // will fail since no credential is found.
    if (CommonUtils.isAlluxioServer()) {
      return gssManager.createCredential(
          null, GSSCredential.DEFAULT_LIFETIME, krb5Mechanism, GSSCredential.ACCEPT_ONLY);
    }
    // Use null GSSName to specify the default principal
    return gssManager.createCredential(
        null, GSSCredential.DEFAULT_LIFETIME, krb5Mechanism, GSSCredential.INITIATE_ONLY);
  }

  /**
   * Gets the Kerberos principal of the login credential from JGSS.
   *
   * @return the Kerberos principal name
   * @throws GSSException if it failed to get the Kerberos principal
   */
  private static String getKerberosPrincipalFromJGSS() throws GSSException {
    GSSManager gssManager = GSSManager.getInstance();
    Oid krb5Mechanism = new Oid(GSSAPI_MECHANISM_ID);

    // Create a temporary INITIATE_ONLY credential just to get the default Kerberos principal,
    // because in an ACCEPT_ONLY credential the principal is always null.
    GSSCredential cred = gssManager.createCredential(
        null, GSSCredential.DEFAULT_LIFETIME, krb5Mechanism, GSSCredential.INITIATE_ONLY);
    String retval = cred.getName().toString();
    // Releases any sensitive information in the temporary GSSCredential
    cred.dispose();
    return retval;
  }

  /**
   * Extracts the {@link KerberosName} in the given {@link Subject}.
   *
   * @param subject the given subject containing the login credentials
   * @return the extracted object
   * @throws LoginException if failed to get Kerberos principal from the login subject
   */
  public static KerberosName extractKerberosNameFromSubject(Subject subject) throws LoginException {
    if (Boolean.getBoolean("sun.security.jgss.native")) {
      try {
        String principal = getKerberosPrincipalFromJGSS();
        Preconditions.checkNotNull(principal);
        return new KerberosName(principal);
      } catch (GSSException e) {
        throw new LoginException("Failed to get the Kerberos principal from JGSS." + e);
      }
    } else {
      Set<KerberosPrincipal> krb5Principals = subject.getPrincipals(KerberosPrincipal.class);
      if (!krb5Principals.isEmpty()) {
        // TODO(chaomin): for now at most one user is supported in one subject. Consider support
        // multiple Kerberos login users in the future.
        return new KerberosName(krb5Principals.iterator().next().toString());
      } else {
        throw new LoginException("Failed to get the Kerberos principal from the login subject.");
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
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
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
          done(new KerberosName(authorizationId).getShortName());
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

  private KerberosUtils() {} // prevent instantiation
}
