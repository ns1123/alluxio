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

package alluxio.security.user;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.Credentials;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.login.KerberosLoginConfiguration;
import alluxio.security.util.KerberosUtils;
import alluxio.util.SecurityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * User state for logging in with JAAS kerberos.
 */
public class JaasKerberosUserState extends BaseUserState {
  private static final Logger LOG = LoggerFactory.getLogger(JaasKerberosUserState.class);
  private static final long MIN_RELOGIN_INTERVAL = 60 * 1000;

  private final String mPrincipal;
  private final String mKeytab;

  private volatile LoginContext mLoginContext = null;

  /**
   * Factory class to create the user state.
   */
  public static class Factory implements UserStateFactory {
    @Override
    public UserState create(Subject subject, AlluxioConfiguration conf, boolean isServer) {
      AuthType authType = conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      if (!authType.equals(AuthType.KERBEROS)) {
        LOG.debug("N/A: auth type is not KERBEROS. authType: {}", authType.getAuthName());
        return null;
      }
      if (Boolean.getBoolean("sun.security.jgss.native")) {
        // jgss native is a different implementation
        LOG.debug("N/A: system property (sun.security.jgss.native) is true");
        return null;
      }
      if (!subject.getPrivateCredentials(Credentials.class).isEmpty()) {
        // Use the token instead
        LOG.debug("N/A: Subject contains a token credential");
        return null;
      }

      // Get Kerberos principal and keytab file from conf.
      String principal = "";
      String keytab = "";
      if (isServer) {
        principal = conf.isSet(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL)
            ? conf.get(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL) : "";
        keytab = conf.isSet(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE)
            ? conf.get(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE) : "";
      } else {
        principal = conf.isSet(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL)
            ? conf.get(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL) : "";
        keytab = conf.isSet(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE)
            ? conf.get(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE) : "";
      }

      return new JaasKerberosUserState(subject, principal, keytab, conf);
    }
  }

  private JaasKerberosUserState(Subject subject, String principal, String keytab,
      AlluxioConfiguration conf) {
    super(subject, conf);
    mPrincipal = principal;
    mKeytab = keytab;
  }

  @Override
  public User login() throws UnauthenticatedException {
    try {
      LOG.debug("Login principal: {} keytab: {}", mPrincipal, mKeytab);

      if (!mPrincipal.isEmpty()) {
        // Add the configured kerberos principal to the existing subject
        mSubject.getPrincipals().add(new KerberosPrincipal(mPrincipal));
      }

      LOG.debug("Using Java kerberos login");
      mLoginContext = jaasLogin(AuthType.KERBEROS, mPrincipal, mKeytab, mSubject);
      LOG.debug("login subject for Kerberos: {}", mSubject);

      try {
        // TODO(gpang): refactor User() so that it does not require conf.
        return new User(mSubject, mConf);
      } catch (IOException e) {
        throw new UnauthenticatedException(e);
      }
    } catch (LoginException e) {
      throw new UnauthenticatedException("Failed to login: " + e.getMessage(), e);
    }
  }

  @Override
  public synchronized User relogin() throws UnauthenticatedException {
    if (mUser == null) {
      return null;
    }
    // Check whether this TGT is sufficiently close to expiration. If there is still more
    // than 60 seconds until expiration, do not attempt to relogin.
    KerberosTicket tgt = getTGT();
    if (tgt != null) {
      long expirationTime = tgt.getEndTime().getTime();
      if (expirationTime - System.currentTimeMillis() >= MIN_RELOGIN_INTERVAL) {
        return mUser;
      }
    }
    if (mLoginContext != null) {
      try {
        mLoginContext.logout();
      } catch (LoginException e) {
        String msg =
            String.format("Failed to log out for %s before relogin attempt", mUser.getName());
        throw new UnauthenticatedException(msg, e);
      }
    }
    try {
      mLoginContext = jaasLogin(AuthType.KERBEROS, mPrincipal, mKeytab, mSubject);
      return new User(mSubject, mConf);
    } catch (LoginException e) {
      String msg = String.format("Failed to login for %s: %s", mUser.getName(), e.getMessage());
      throw new UnauthenticatedException(msg, e);
    } catch (IOException e) {
      // failed to construct User()
      throw new UnauthenticatedException(e);
    }
  }

  private KerberosTicket getTGT() {
    if (mUser == null) {
      return null;
    }
    return KerberosUtils.extractOriginalTGTFromSubject(mSubject);
  }

  private static LoginContext jaasLogin(AuthType authType, String principal, String keytab,
      Subject subject) throws LoginException {
    Configuration loginConf = new KerberosLoginConfiguration(principal, keytab);
    LoginContext loginContext = SecurityUtils
        .createLoginContext(authType, subject, KerberosPrincipal.class.getClassLoader(), loginConf,
            null);
    try {
      loginContext.login();
    } catch (LoginException e) {
      // Run some diagnostics on the keytab file
      boolean exists = false;
      boolean accessible = false;
      File keytabFile = new java.io.File(keytab);
      if (keytabFile.exists()) {
        exists = true;
      }
      if (exists && Files.isReadable(keytabFile.toPath())) {
        accessible = true;
      }
      String principalMsg = principal.isEmpty() ? "<not specified>" : principal;
      String keytabMsg = keytab.isEmpty() ? "<not specified>" : keytab;
      String keytabInfo = String
          .format("%s (principal: %s keytab: %s keytabExists: %b keytabAccessible: %b)",
              e.getMessage().trim(), principalMsg, keytabMsg, exists, accessible);
      throw new LoginException(keytabInfo);
    }
    Set<KerberosPrincipal> krb5Principals = subject.getPrincipals(KerberosPrincipal.class);
    if (krb5Principals.isEmpty()) {
      throw new LoginException("Kerberos login failed: login subject has no principal.");
    }
    return loginContext;
  }
}
