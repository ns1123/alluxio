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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.netty.NettyAttributes;
import alluxio.proto.security.CapabilityProto;
import alluxio.security.Credentials;
import alluxio.security.MasterKey;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authentication.DelegationTokenIdentifier;
import alluxio.security.authentication.ImpersonationAuthenticator;
import alluxio.security.authentication.Token;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoUtils;

import io.netty.channel.Channel;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.apache.commons.codec.binary.Base64;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;

/**
 * Utils for Kerberos.
 */
public final class KerberosUtils {
  public static final String DIGEST_MECHANISM_NAME = "DIGEST-MD5";
  public static final String GSSAPI_MECHANISM_NAME = "GSSAPI";
  // The constant below identifies the Kerberos v5 GSS-API mechanism type, see
  // https://docs.oracle.com/javase/7/docs/api/org/ietf/jgss/GSSManager.html for details
  public static final String GSSAPI_MECHANISM_ID = "1.2.840.113554.1.2.2";
  private static final Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);

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
   * @param conf Alluxio configuration
   * @return the Kerberos service name
   */
  public static String getKerberosServiceName(AlluxioConfiguration conf) {
    String kerberosServiceName = conf.get(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME);
    if (!kerberosServiceName.isEmpty()) {
      return kerberosServiceName;
    }
    throw new RuntimeException(
        PropertyKey.SECURITY_KERBEROS_SERVICE_NAME.toString() + " must be set.");
  }

  /**
   * @param conf Alluxio configuration
   * @return the Kerberos unified instance name if set, otherwise null
   */
  public static String maybeGetKerberosUnifiedInstanceName(AlluxioConfiguration conf) {
    if (!conf.isSet(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME)) {
      return null;
    }
    String unifedInstance = conf.get(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME);
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
   * Extract the original ticket granting ticket (TGT) from the given {@link Subject}.
   *
   * @param subject the {@link Subject} from which to extract Kerberos TGT
   * @return the original TGT of this subject
   */
  public static KerberosTicket extractOriginalTGTFromSubject(Subject subject) {
    if (subject == null) {
      return null;
    }
    Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
    for (KerberosTicket ticket : tickets) {
      KerberosPrincipal serverPrincipal = ticket.getServer();
      if (serverPrincipal != null && serverPrincipal.getName().equals(
          "krbtgt/" + serverPrincipal.getRealm() + "@" + serverPrincipal.getRealm())) {
        return ticket;
      }
    }
    return null;
  }

  /**
   * Gets the delegation token from a subject.
   *
   * @param subject subject which contains the delegation token
   * @param addr the Alluxio master address which the token is associated with
   * @return the delegation token if found, or null if not
   */
  public static Token<DelegationTokenIdentifier> getDelegationToken(Subject subject,
      String addr) {
    LOG.debug("getting delegation tokens for subject {}", subject);
    if (subject == null) {
      return null;
    }
    synchronized (subject) {
      Set<Credentials> allCredentials = subject.getPrivateCredentials(Credentials.class);
      if (allCredentials.isEmpty()) {
        LOG.debug("no Alluxio credentials found.");
        return null;
      }
      Credentials credentials = allCredentials.iterator().next();
      Token<DelegationTokenIdentifier> token = credentials.getToken(addr);
      LOG.debug("got delegation token: {}", token);
      return token;
    }
  }

  /**
   * Sets the client user names for the corresponding thrift RPC call.
   *
   * @param user the client user who is making the request
   * @param connectionUser the actual user who is authenticating on behalf of the request user. Can
   *                       be null if the client user is authenticated using own credentials.
   * @param authMethod the authentication method used by the client
   */
  private static void updateThriftRpcUsers(String user, String connectionUser, String authMethod,
      AlluxioConfiguration conf) {
    try {
      User oldUser = AuthenticatedClientUser.get(conf);
      Preconditions
          .checkState(oldUser == null, "A user (%s) exists while adding user (%s).", oldUser,
              user);
    } catch (IOException e) {
      // This should never happen.
      throw Throwables.propagate(e);
    }

    AuthenticatedClientUser.setAuthMethod(authMethod);
    AuthenticatedClientUser.set(user);
    if (connectionUser != null) {
      AuthenticatedClientUser.setConnectionUser(connectionUser);
    }
  }

  /**
   * CallbackHandler for SASL GSSAPI Kerberos mechanism.
   */
  private abstract static class AbstractGssSaslCallbackHandler implements CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractGssSaslCallbackHandler.class);
    private final ImpersonationAuthenticator mImpersonationAuthenticator;

    protected final AlluxioConfiguration mConf;

    /**
     * Creates a new instance of {@link AbstractGssSaslCallbackHandler}.
     */
    public AbstractGssSaslCallbackHandler(AlluxioConfiguration conf) {
      mImpersonationAuthenticator = new ImpersonationAuthenticator(conf);
      mConf = conf;
    }

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
        String authenticationId = ac.getAuthenticationID();
        String authorizationId = ac.getAuthorizationID();

        try {
          authorizationId = new KerberosName(authorizationId)
              .getShortName(mConf.get(PropertyKey.SECURITY_KERBEROS_AUTH_TO_LOCAL));
        } catch (Exception e) {
          // Ignore, since the impersonation user is not guaranteed to be a Kerberos name
        }

        String connectionUser;
        try {
          connectionUser = new KerberosName(authenticationId)
              .getShortName(mConf.get(PropertyKey.SECURITY_KERBEROS_AUTH_TO_LOCAL));
          mImpersonationAuthenticator
              .authenticate(connectionUser, authorizationId);
          ac.setAuthorized(true);
        } catch (Exception e) {
          // Logging here to show the error on the master. Otherwise, error messages get swallowed.
          LOG.error("Impersonation failed.", e);
          ac.setAuthorized(false);
          throw e;
        }

        if (ac.isAuthorized()) {
          ac.setAuthorizedID(authorizationId);
          done(new KerberosName(authorizationId)
                  .getShortName(mConf.get(PropertyKey.SECURITY_KERBEROS_AUTH_TO_LOCAL)),
              connectionUser);
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
    protected abstract void done(String user, String connectionUser);
  }

  /**
   * The kerberos sasl callback for the thrift servers.
   */
  public static final class ThriftGssSaslCallbackHandler extends AbstractGssSaslCallbackHandler {
    private final Runnable mCallback;

    /**
     * Creates a {@link ThriftGssSaslCallbackHandler} instance.
     *
     * @param conf Alluxio configuration
     * @param callback the callback runs after the connection is authenticated
     */
    public ThriftGssSaslCallbackHandler(AlluxioConfiguration conf, Runnable callback) {
      super(conf);
      mCallback = callback;
    }

    @Override
    protected void done(String user, String connectionUser) {
      // After verification succeeds, a user with this authorizationId will be set to a
      // Threadlocal.
      updateThriftRpcUsers(user, connectionUser, GSSAPI_MECHANISM_NAME, mConf);
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
     * @param conf Alluxio configuration
     * @param channel the netty channel
     */
    public NettyGssSaslCallbackHandler(AlluxioConfiguration conf, Channel channel) {
      super(conf);
      mChannel = channel;
    }

    @Override
    protected void done(String user, String connectionUser) {
      mChannel.attr(NettyAttributes.CHANNEL_KERBEROS_USER_KEY).set(user);
    }
  }

  /**
   * The delegation token SASL callback for servers.
   */
  public abstract static class SaslDigestServerCallbackHandler implements CallbackHandler {
    protected abstract void authorize(AuthorizeCallback ac) throws IOException;

    protected abstract char[] getPassword(String name);

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      NameCallback nc = null;
      PasswordCallback pc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          // ignore realms
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST Callback");
        }
      }

      if (pc != null) {
        LOG.debug("SaslDigestServerCallbackHandler: Retrieving password for {}", nc.getDefaultName());
        char[] password = getPassword(nc.getDefaultName());
        pc.setPassword(password);
      }

      if (ac != null) {
        authorize(ac);
        if (ac.isAuthorized()) {
          LOG.debug("SaslDigestServerCallbackHandler: user {} successfully authorized", ac.getAuthorizedID());
        }
      }
    }
  }

  /**
   * Digest sasl callback for the netty servers.
   */
  public static class NettyDigestServerCallbackHandler
      extends SaslDigestServerCallbackHandler {
    private Channel mChannel;
    private List<MasterKey> mActiveMasterKeys;

    /**
     * @param channel the channel
     * @param activeMasterKeys list of active master keys that are used for signing capabilities
     */
    public NettyDigestServerCallbackHandler(Channel channel, List<MasterKey> activeMasterKeys) {
      mChannel = channel;
      mActiveMasterKeys = activeMasterKeys;
    }

    @Override
    protected void authorize(AuthorizeCallback ac) throws IOException {
      ac.setAuthorized(true);
      // Extract from authorized user, which is a capability content, the username
      // and save it in the channel for next handlers.
      byte[] contentProto = Base64.decodeBase64(ac.getAuthorizedID());
      CapabilityProto.Content contentDec = ProtoUtils.decode(contentProto);
      String capabilityUser = contentDec.getUser();
      mChannel.attr(NettyAttributes.CHANNEL_KERBEROS_USER_KEY).set(capabilityUser);
    }

    @Override
    protected char[] getPassword(String name) {
      try {
        // Extract keyId from the user word that is being authenticated
        byte[] contentProto = Base64.decodeBase64(name);
        CapabilityProto.Content contentDec = ProtoUtils.decode(contentProto);
        long keyId = contentDec.getKeyId();
        MasterKey clientMasterKey = null;
        for (MasterKey serverKey : mActiveMasterKeys) {
          if (serverKey.getKeyId() == keyId) {
            LOG.debug("Found a master key for received client content with Id:{}", keyId);
            clientMasterKey = serverKey;
            break;
          }
        }
        Preconditions.checkNotNull(clientMasterKey,
                "Failed to find master key:{} among active server keys.", keyId);

        byte[] authenticator = clientMasterKey.calculateHMAC(contentProto);
        String strAuthenticatorEnc =
                new String(Base64.encodeBase64(authenticator, false), Charsets.UTF_8);
        return strAuthenticatorEnc.toCharArray();
      } catch (Exception e) {
        LOG.error("Cannot obtain password", e);
        return new char[0];
      }
    }
  }

  /**
   * SASL client callback handler for DIGEST-MD5 based authentication.
   */
  public static class SaslDigestClientCallbackHandler implements CallbackHandler {
    private final String mUserName;
    private final char[] mUserPassword;

    /**
     * @param token token used for authentication
     */
    public SaslDigestClientCallbackHandler(Token<?> token) {
      mUserName = buildUserName(token);
      mUserPassword = buildPassword(token);
    }

    /**
     * @param userName username
     * @param userPassword password
     */
    public SaslDigestClientCallbackHandler(final String userName, final String userPassword) {
      // TODO(ggezer) Send a token from caller side to avoid extra _ctor.
      mUserName = userName;
      mUserPassword = userPassword.toCharArray();
    }

    private static String buildUserName(Token<?> token) {
      byte[] proto = token.getId().getBytes();
      return new String(Base64.encodeBase64(proto, false), Charsets.UTF_8);
    }

    private char[] buildPassword(Token<?> token) {
      return new String(Base64.encodeBase64(token.getPassword(), false),
          Charsets.UTF_8).toCharArray();
    }

    @Override
    public void handle(Callback[] callbacks)
        throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST callback");
        }
      }
      if (nc != null) {
        LOG.debug("SaslDigestClientCallbackHandler: setting username to {}", mUserName);
        nc.setName(mUserName);
      }
      if (pc != null) {
        LOG.debug("SaslDigestClientCallbackHandler: setting password");
        pc.setPassword(mUserPassword);
      }
      if (rc != null) {
        LOG.debug("SaslDigestClientCallbackHandler: setting realm to {}", rc.getDefaultText());
        rc.setText(rc.getDefaultText());
      }
    }
  }

  private KerberosUtils() {} // prevent instantiation
}
