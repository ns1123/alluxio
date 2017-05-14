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

  private LoginUser() {} // prevent instantiation

  /**
   * Gets current singleton login user. This method is called to identify the singleton user who
   * runs Alluxio client. When Alluxio client gets a user by this method and connects to Alluxio
   * service, this user represents the client and is maintained in service.
   *
   * @return the login user
   */
<<<<<<< HEAD
  public static User get() {
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
    try {
      if (alluxio.util.CommonUtils.isAlluxioServer()) {
        return getServerUser();
      } else {
        return getClientUser();
      }
    } catch (java.io.IOException e) {
      throw new UnauthenticatedException(e);
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
   * @throws java.io.IOException if login fails
   */
  public static User getClientUser() throws java.io.IOException {
    return getUserWithConf(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL,
        PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
  }

  /**
   * Same as {@link LoginUser#get} except that server-side login uses SECURITY_KERBEROS_SERVER
   * config params. Server-side Kerberos principal and keytab files must be set correctly because
   * Alluxio servers must login from Keytab files.
   *
   * @return the login user
   * @throws java.io.IOException if login fails
   */
  public static User getServerUser() throws java.io.IOException {
    return getUserWithConf(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL,
        PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE);
  }

  /**
   * Helper function for {@link LoginUser#getClientUser()} and
   * {@link LoginUser#getServerUser()}.
   *
   * @param principalKey conf key of Kerberos principal for the login user
   * @param keytabKey conf key of Kerberos keytab file path for the login user
   * @return the login user
   * @throws java.io.IOException if login fails
   */
  private static User getUserWithConf(PropertyKey principalKey, PropertyKey keytabKey)
      throws java.io.IOException {
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

||||||| merged common ancestors
  public static User get() {
=======
  public static User get() throws UnauthenticatedException {
>>>>>>> OPENSOURCE/master
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
        } else {
          // Use Java native Kerberos library to login
          LoginModuleConfiguration loginConf = new LoginModuleConfiguration(principal, keytab);

          LoginContext loginContext = createLoginContext(authType, subject,
              javax.security.auth.kerberos.KerberosPrincipal.class.getClassLoader(), loginConf);
          loginContext.login();

          Set<javax.security.auth.kerberos.KerberosPrincipal> krb5Principals =
              subject.getPrincipals(javax.security.auth.kerberos.KerberosPrincipal.class);
          if (krb5Principals.isEmpty()) {
            throw new LoginException("Kerberos login failed: login subject has no principals.");
          }
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

  /**
   * Gets the client login subject if and only if the secure mode is {Authtype#KERBEROS}. Otherwise
   * returns null.
   *
   * @return login Subject if AuthType is KERBEROS, otherwise null
   * @throws java.io.IOException if the login failed
   */
  public static Subject getClientLoginSubject() throws java.io.IOException {
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
   * @throws java.io.IOException if the login failed
   */
  public static Subject getServerLoginSubject() throws java.io.IOException {
    if (Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        != AuthType.KERBEROS) {
      return null;
    }
    return getServerUser().getSubject();
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
