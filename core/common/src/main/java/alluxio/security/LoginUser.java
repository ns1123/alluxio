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

package alluxio.security;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.authentication.AuthType;
import alluxio.security.login.AppLoginModule;
import alluxio.security.login.LoginModuleConfiguration;

import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * A Singleton of LoginUser, which is an instance of {@link alluxio.security.User}. It represents
 * the user of Alluxio client, when connecting to Alluxio service.
 *
 * The implementation of getting a login user supports Windows, Unix, and Kerberos login modules.
 *
 * This singleton uses lazy initialization.
 */
@ThreadSafe
public final class LoginUser {

  /** User instance of the login user in Alluxio client process. */
  private static User sLoginUser;
  // ALLUXIO CS ADD
  private static LoginContext sLoginContext;
  private static String sPrincipal;
  private static String sKeytab;
  /** Provider of external Kerberos credentials allows hooking up with 3rd party login functions. */
  private static alluxio.security.authentication.KerberosLoginProvider sExternalLoginProvider;
  /**
   * Minimum time interval until next login attempt is 60 seconds.
   */
  private static final long MIN_RELOGIN_INTERVAL = 60 * 1000;
  // ALLUXIO CS END

  private LoginUser() {} // prevent instantiation

  /**
   * Gets current singleton login user. This method is called to identify the singleton user who
   * runs Alluxio client. When Alluxio client gets a user by this method and connects to Alluxio
   * service, this user represents the client and is maintained in service.
   *
   * @return the login user
   */
  public static User get() throws UnauthenticatedException {
    // ALLUXIO CS REPLACE
    // if (sLoginUser == null) {
    //   synchronized (LoginUser.class) {
    //     if (sLoginUser == null) {
    //       sLoginUser = login();
    //     }
    //   }
    // }
    // return sLoginUser;
    // ALLUXIO CS WITH
    if (alluxio.util.CommonUtils.isAlluxioServer()) {
      return getServerUser();
    } else {
      return getClientUser();
    }
    // ALLUXIO CS END
  }
  // ALLUXIO CS ADD

  /**
   * Same as {@link LoginUser#get} except that client-side login uses SECURITY_KERBEROS_CLIENT
   * config params. Client-side Kerberos principal and keytab files can be empty because client
   * login can be from either keytab files or kinit ticket cache on client machine.
   *
   * @return the login user
   */
  public static User getClientUser() throws UnauthenticatedException {
    return getUserWithConf(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL,
        PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
  }

  /**
   * Same as {@link LoginUser#get} except that server-side login uses SECURITY_KERBEROS_SERVER
   * config params. Server-side Kerberos principal and keytab files must be set correctly because
   * Alluxio servers must login from Keytab files.
   *
   * @return the login user
   */
  public static User getServerUser() throws UnauthenticatedException {
    return getUserWithConf(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL,
        PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE);
  }

  /**
   * Sets the external login provider that provides alternative login functionality.
   *
   * @param provider an external Kerberos login provider
   */
  public static void setExternalLoginProvider(
      alluxio.security.authentication.KerberosLoginProvider provider) {
    sExternalLoginProvider = provider;
  }

  /**
   * Helper function for {@link LoginUser#getClientUser()} and
   * {@link LoginUser#getServerUser()}.
   *
   * @param principalKey conf key of Kerberos principal for the login user
   * @param keytabKey conf key of Kerberos keytab file path for the login user
   * @return the login user
   */
  private static User getUserWithConf(PropertyKey principalKey, PropertyKey keytabKey)
      throws UnauthenticatedException {
    if (Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        != AuthType.KERBEROS) {
      if (sLoginUser == null) {
        synchronized (LoginUser.class) {
          if (sLoginUser == null) {
            sLoginUser = login();
          }
        }
      }
      return sLoginUser;
    }

    if (sLoginUser == null) {
      synchronized (LoginUser.class) {
        if (sLoginUser == null) {
          Configuration.set(PropertyKey.SECURITY_KERBEROS_LOGIN_PRINCIPAL,
              Configuration.get(principalKey));
          Configuration.set(PropertyKey.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE,
              Configuration.get(keytabKey));
          sLoginUser = login();
        }
      }
    }
    return sLoginUser;
  }
  // ALLUXIO CS END

  /**
   * Logs in based on the LoginModules.
   *
   * @return the login user
   */
  private static User login() throws UnauthenticatedException {
    AuthType authType =
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    checkSecurityEnabled(authType);
    Subject subject = new Subject();

    try {
      // ALLUXIO CS ADD
      if (authType.equals(AuthType.KERBEROS)) {
        // Get Kerberos principal and keytab file from conf.
        String principal = Configuration.get(PropertyKey.SECURITY_KERBEROS_LOGIN_PRINCIPAL);
        String keytab = Configuration.get(PropertyKey.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE);

        if (!principal.isEmpty()) {
          subject = new Subject(false,
              com.google.common.collect.Sets.newHashSet(
                  new javax.security.auth.kerberos.KerberosPrincipal(principal)),
              new java.util.HashSet<Object>(), new java.util.HashSet<Object>());
        }

        if (Boolean.getBoolean("sun.security.jgss.native")) {
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
            throw new LoginException("javax.security.auth.useSubjectCredsOnly must be set to false "
                + "in order to use native platform GSS integration.");
          }

          try {
            org.ietf.jgss.GSSCredential cred =
                alluxio.security.util.KerberosUtils.getCredentialFromJGSS();
            subject.getPrivateCredentials().add(cred);
          } catch (org.ietf.jgss.GSSException e) {
            throw new LoginException("Cannot add private credential to subject with JGSS: " + e);
          }
        } else if (principal.isEmpty() && sExternalLoginProvider != null
            && sExternalLoginProvider.hasKerberosCrendentials()) {
          // Try external login if a Kerberos principal is not provided in configuration.
          subject = sExternalLoginProvider.login();
        } else {
          // Use Java Kerberos library to login
          sLoginContext = jaasLogin(authType, principal, keytab, subject);
          sPrincipal = principal;
          sKeytab = keytab;
        }

        try {
          return new User(subject);
        } catch (java.io.IOException e) {
          throw new UnauthenticatedException(e);
        }
      }
      // ALLUXIO CS END
      // Use the class loader of User.class to construct the LoginContext. LoginContext uses this
      // class loader to dynamically instantiate login modules. This enables
      // Subject#getPrincipals to use reflection to search for User.class instances.
      LoginContext loginContext = createLoginContext(authType, subject, User.class.getClassLoader(),
          new LoginModuleConfiguration());
      loginContext.login();
    } catch (LoginException e) {
      throw new UnauthenticatedException("Failed to login: " + e.getMessage(), e);
    }

    Set<User> userSet = subject.getPrincipals(User.class);
    if (userSet.isEmpty()) {
      throw new UnauthenticatedException("Failed to login: No Alluxio User is found.");
    }
    if (userSet.size() > 1) {
      StringBuilder msg = new StringBuilder(
          "Failed to login: More than one Alluxio Users are found:");
      for (User user : userSet) {
        msg.append(" ").append(user.toString());
      }
      throw new UnauthenticatedException(msg.toString());
    }
    return userSet.iterator().next();
  }
  // ALLUXIO CS ADD

  /**
   * Performs relogin for {@link #sLoginUser}.
   *
   * This method retrieves the previous {@link LoginContext} used by {@link #sLoginUser},
   * logs out, and performs login again.
   */
  public static synchronized void relogin() throws UnauthenticatedException {
    AuthType authType =
        Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    Subject subject = sLoginUser.getSubject();
    // Relogin is required only for Kerberos authentication due to ticket expiration.
    // When authType is SIMPLE, it's not needed.
    if (!authType.equals(AuthType.KERBEROS)) {
      return;
    }
    // Furthermore, if the user sets sun.security.jgss.native to true, it's assumed that
    // the user will take the responsibility of keeping the tickets in the ticket
    // cache always valid. Therefore, Alluxio does not perform automatic relogin in
    // this case at the moment.
    if (Boolean.getBoolean("sun.security.jgss.native")) {
      // TODO(gene) maybe implement the relogin via native JGSS
      return;
    }
    if (com.google.common.base.Strings.isNullOrEmpty(sPrincipal)
        && sExternalLoginProvider != null && sExternalLoginProvider.hasKerberosCrendentials()) {
      try {
        sExternalLoginProvider.relogin();
      } catch (LoginException e) {
        String msg = String.format("Failed to relogin user %s using external login provider.",
            sLoginUser.getName());
        throw new UnauthenticatedException(msg, e);
      }
      return;
    }
    // Check whether this TGT is sufficiently close to expiration. If there is still more
    // than 60 seconds until expiration, do not attempt to relogin.
    javax.security.auth.kerberos.KerberosTicket tgt = getTGT();
    if (tgt != null) {
      long expirationTime = tgt.getEndTime().getTime();
      if (expirationTime - System.currentTimeMillis() >= MIN_RELOGIN_INTERVAL) {
        return;
      }
    }
    if (sLoginContext != null) {
      try {
        sLoginContext.logout();
      } catch (LoginException e) {
        String msg = String.format("Failed to log out for %s before relogin attempt",
            sLoginUser.getName());
        throw new UnauthenticatedException(msg, e);
      }
    }
    try {
      sLoginContext = jaasLogin(authType, sPrincipal, sKeytab, subject);
    } catch (LoginException e) {
      String msg = String.format("Failed to login for %s: %s",
          sLoginUser.getName(), e.getMessage());
      throw new UnauthenticatedException(msg, e);
    }
  }
  // ALLUXIO CS END

  /**
   * Checks whether Alluxio is running in secure mode, such as {@link AuthType#SIMPLE},
   * {@link AuthType#KERBEROS}, {@link AuthType#CUSTOM}.
   *
   * @param authType the authentication type in configuration
   */
  private static void checkSecurityEnabled(AuthType authType) {
    // ALLUXIO CS ADD
    if (authType == AuthType.KERBEROS) {
      return;
    }
    // ALLUXIO CS END
    // TODO(dong): add Kerberos condition check.
    if (authType != AuthType.SIMPLE && authType != AuthType.CUSTOM) {
      throw new UnsupportedOperationException("User is not supported in " + authType.getAuthName()
          + " mode");
    }
  }
  // ALLUXIO CS ADD

  private static LoginContext jaasLogin(AuthType authType, String principal, String keytab,
      Subject subject) throws LoginException {
    LoginModuleConfiguration loginConf = new LoginModuleConfiguration(principal, keytab);
    LoginContext loginContext = createLoginContext(authType, subject,
        javax.security.auth.kerberos.KerberosPrincipal.class.getClassLoader(), loginConf);
    loginContext.login();
    Set<javax.security.auth.kerberos.KerberosPrincipal> krb5Principals =
        subject.getPrincipals(javax.security.auth.kerberos.KerberosPrincipal.class);
    if (krb5Principals.isEmpty()) {
      throw new LoginException("Kerberos login failed: login subject has no principal.");
    }
    return loginContext;
  }

  /**
   * Gets the client login subject if and only if the secure mode is {Authtype#KERBEROS}. Otherwise
   * returns null.
   *
   * @return login Subject if AuthType is KERBEROS, otherwise null
   */
  public static Subject getClientLoginSubject() throws UnauthenticatedException {
    if (Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        != AuthType.KERBEROS) {
      return null;
    }
    return getClientUser().getSubject();
  }

  /**
   * Gets the server login subject if and only if the secure mode is {Authtype#KERBEROS}. Otherwise
   * returns null.
   *
   * @return login Subject if AuthType is KERBEROS, otherwise null
   */
  public static Subject getServerLoginSubject() throws UnauthenticatedException {
    if (Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        != AuthType.KERBEROS) {
      return null;
    }
    return getServerUser().getSubject();
  }

  private static javax.security.auth.kerberos.KerberosTicket getTGT() {
    if (sLoginUser == null) {
      return null;
    }
    return alluxio.security.util.KerberosUtils
        .extractOriginalTGTFromSubject(sLoginUser.getSubject());
  }
  // ALLUXIO CS END

  /**
   * Creates a new {@link LoginContext} with the correct class loader.
   *
   * @param authType the {@link AuthType} to use
   * @param subject the {@link Subject} to use
   * @param classLoader the {@link ClassLoader} to use
   * @param configuration the {@link javax.security.auth.login.Configuration} to use
   * @return the new {@link LoginContext} instance
   * @throws LoginException if LoginContext cannot be created
   */
  private static LoginContext createLoginContext(AuthType authType, Subject subject,
      ClassLoader classLoader, javax.security.auth.login.Configuration configuration)
      throws LoginException {
    CallbackHandler callbackHandler = null;
    if (authType.equals(AuthType.SIMPLE) || authType.equals(AuthType.CUSTOM)) {
      callbackHandler = new AppLoginModule.AppCallbackHandler();
    }

    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    try {
      // Create LoginContext based on authType, corresponding LoginModule should be registered
      // under the authType name in LoginModuleConfiguration.
      return new LoginContext(authType.getAuthName(), subject, callbackHandler, configuration);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }
}
