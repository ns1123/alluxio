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
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.security.util.KerberosUtils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

/**
 * User state for logging in with kerberos via native libraries.
 */
public class NativeKerberosUserState extends BaseUserState {
  private static final Logger LOG = LoggerFactory.getLogger(NativeKerberosUserState.class);

  private final String mPrincipal;
  private final String mKeytab;

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
      if (!Boolean.getBoolean("sun.security.jgss.native")) {
        LOG.debug("N/A: system property (sun.security.jgss.native) is not true");
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

      return new NativeKerberosUserState(subject, principal, keytab, conf);
    }
  }

  private NativeKerberosUserState(Subject subject, String principal, String keytab,
      AlluxioConfiguration conf) {
    super(subject, conf);
    mPrincipal = principal;
    mKeytab = keytab;
  }

  @Override
  public User login() throws UnauthenticatedException {
    LOG.debug("Using native library (sun.security.jgss.native)");
    // Use MIT native Kerberos library
    // http://docs.oracle.com/javase/6/docs/technotes/guides/security/jgss/jgss-features.html
    // Unlike the default Java GSS implementation which relies on JAAS KerberosLoginModule
    // for initial credential acquisition, when using native GSS, the initial credential
    // should be acquired beforehand, e.g. kinit, prior to calling JGSS APIs.
    //
    // Note: when "sun.security.jgss.native" is set to true, it is required to set
    // "javax.security.auth.useSubjectCredsOnly" to false. This relaxes the restriction of
    // requiring a GSS mechanism to obtain necessary credentials from JAAS.
    if (Boolean.getBoolean("javax.security.auth.useSubjectCredsOnly")) {
      throw new UnauthenticatedException(
          "javax.security.auth.useSubjectCredsOnly must be set to false "
              + "in order to use native platform GSS integration.");
    }

    try {
      GSSCredential cred = KerberosUtils.getCredentialFromJGSS();
      mSubject.getPrivateCredentials().add(cred);
    } catch (GSSException e) {
      if ((!alluxio.util.CommonUtils.isAlluxioServer()) && mConf
          .getBoolean(PropertyKey.SECURITY_KERBEROS_CLIENT_TICKETCACHE_LOGIN_ENABLED)) {
        // attempts to obtain a valid Kerberos ticket by using kinit command
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("KRB5CCNAME = {}", System.getenv("KRB5CCNAME"));
            LOG.debug("KRB5_CONFIG = {}", System.getenv("KRB5_CONFIG"));
          }
          if (StringUtils.isEmpty(mKeytab)) {
            throw new LoginException("Cannot perform a kinit: keytab is not set.");
          }
          if (StringUtils.isEmpty(mPrincipal)) {
            throw new LoginException("Cannot perform a kinit: principal is not set.");
          }
          String[] cmd = new String[] {"kinit", "-kt", mKeytab, mPrincipal};
          if (LOG.isDebugEnabled()) {
            LOG.debug("Running kinit command: {}", Arrays.toString(cmd));
          }
          Process p = Runtime.getRuntime().exec(cmd);
          if (!p.waitFor(30, TimeUnit.SECONDS)) {
            LOG.warn("Timeout after 30 seconds while waiting for kinit to complete.");
          }
          if (LOG.isDebugEnabled()) {
            String stdout = IOUtils.toString(p.getInputStream());
            LOG.debug("kinit stdout: {}", stdout);
            String stderr = IOUtils.toString(p.getErrorStream());
            LOG.debug("kinit stderr: {}", stderr);
          }
          GSSCredential cred = KerberosUtils.getCredentialFromJGSS();
          mSubject.getPrivateCredentials().add(cred);
          LOG.debug("kinit succeeded");
          e = null;
        } catch (Exception e2) {
          LOG.error(
              "Error running kinit to obtain ticket from JGSS: {}, principal = {}, keytab = {}",
              e2.getMessage(), mPrincipal, mKeytab);
        }
      }
      if (e != null) {
        throw new UnauthenticatedException(String.format(
            "Cannot add private credential to subject with JGSS: %s, principal = %s, keytab = %s",
            e.getMessage(), mPrincipal, mKeytab));
      }
    }
    try {
      return new User(mSubject, mConf);
    } catch (LoginException e) {
      throw new UnauthenticatedException("Failed to login: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new UnauthenticatedException(e);
    }
  }

  @Override
  public synchronized User relogin() throws UnauthenticatedException {
    try {
      return login();
    } catch (UnauthenticatedException e) {
      String msg = String
          .format("Failed to relogin user %s using jgss. msg: %s", mUser.getName(), e.getMessage());
      throw new UnauthenticatedException(msg, e);
    }
  }
}
