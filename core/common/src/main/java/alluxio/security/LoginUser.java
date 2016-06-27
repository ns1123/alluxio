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
import alluxio.Constants;
import alluxio.security.authentication.AuthType;
import alluxio.security.login.AppLoginModule;
import alluxio.security.login.LoginModuleConfiguration;
// ENTERPRISE ADD

import com.google.common.collect.Sets;
// ENTERPRISE END

import java.io.IOException;
// ENTERPRISE ADD
import java.util.HashSet;
// ENTERPRISE END
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
// ENTERPRISE ADD
import javax.security.auth.kerberos.KerberosPrincipal;
// ENTERPRISE END
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
   * @param conf Alluxio configuration
   * @return the login user
   * @throws java.io.IOException if login fails
   */
  public static User get(Configuration conf) throws IOException {
    // ENTERPRISE EDIT
    // TODO(chaomin): consider adding a JVM-level constant to distinguish between Alluxio server
    // and client. It's brittle to depend on alluxio.logger.type.
    String loggerType = conf.get(Constants.LOGGER_TYPE);
    if (loggerType.equalsIgnoreCase("MASTER_LOGGER")
        || loggerType.equalsIgnoreCase("WORKER_LOGGER)")) {
      return getServerUser(conf);
    } else {
      return getClientUser(conf);
    }
    // ENTERPRISE REPLACES
    // if (sLoginUser == null) {
    //  synchronized (LoginUser.class) {
    //    if (sLoginUser == null) {
    //      sLoginUser = login(conf);
    //    }
    //  }
    // }
    // return sLoginUser;
    // ENTERPRISE END
  }
  // ENTERPRISE ADD

  /**
   * Same as {@link LoginUser#get} except that client-side login uses SECURITY_KERBEROS_CLIENT
   * config params.
   *
   * @param conf Alluxio configuration
   * @return the login user
   * @throws java.io.IOException if login fails
   */
  public static User getClientUser(Configuration conf) throws IOException {
    return getUserWithConf(conf, Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL,
        Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
  }

  /**
   * Same as {@link LoginUser#get} except that server-side login uses SECURITY_KERBEROS_SERVER
   * config params.
   *
   * @param conf Alluxio configuration
   * @return the login user
   * @throws java.io.IOException if login fails
   */
  public static User getServerUser(Configuration conf) throws IOException {
    return getUserWithConf(conf, Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL,
        Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE);
  }

  /**
   * Helper function for {@link LoginUser#getClientUser(Configuration)} and
   * {@link LoginUser#getServerUser(Configuration)}.
   *
   * @param conf Alluxio configuration
   * @param principalKey conf key of Kerberos principal for the login user
   * @param keytabKey conf key of Kerberos keytab file path for the login user
   * @return the login user
   * @throws java.io.IOException if login fails
   */
  private static User getUserWithConf(Configuration conf, String principalKey, String keytabKey)
      throws IOException {
    if (conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class)
        != AuthType.KERBEROS) {
      if (sLoginUser == null) {
        synchronized (LoginUser.class) {
          if (sLoginUser == null) {
            sLoginUser = login(conf);
          }
        }
      }
      return sLoginUser;
    }

    if (!conf.containsKey(principalKey) || conf.get(principalKey).isEmpty()) {
      throw new IOException("Invalid config: " + principalKey + " must be set.");
    }
    if (!conf.containsKey(keytabKey) || conf.get(keytabKey).isEmpty()) {
      throw new IOException("Invalid config: " + keytabKey + " must be set.");
    }

    if (sLoginUser == null) {
      synchronized (LoginUser.class) {
        if (sLoginUser == null) {
          Configuration krb5LoginConf = conf;
          krb5LoginConf.set(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL, conf.get(principalKey));
          krb5LoginConf.set(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE, conf.get(keytabKey));
          sLoginUser = login(krb5LoginConf);
        }
      }
    }
    return sLoginUser;
  }
  // ENTERPRISE END

  /**
   * Logs in based on the LoginModules.
   *
   * @param conf Alluxio configuration
   * @return the login user
   * @throws IOException if login fails
   */
  private static User login(Configuration conf) throws IOException {
    AuthType authType = conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    checkSecurityEnabled(authType);

    try {
      Subject subject = new Subject();

      CallbackHandler callbackHandler = null;
      // ENTERPRISE ADD
      if (authType.equals(AuthType.KERBEROS)) {
        // Get Kerberos principal and keytab file from conf.
        if (!conf.containsKey(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL)) {
          throw new LoginException("Kerberos login failed: "
              + Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL + " must be set.");
        }
        if (!conf.containsKey(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE)) {
          throw new LoginException("Kerberos login failed: "
              + Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE + " must be set.");
        }
        String principal = conf.get(Constants.SECURITY_KERBEROS_LOGIN_PRINCIPAL);
        String keytab = conf.get(Constants.SECURITY_KERBEROS_LOGIN_KEYTAB_FILE);

        subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(principal)),
            new HashSet<Object>(), new HashSet<Object>());
        LoginModuleConfiguration loginConf = new LoginModuleConfiguration(principal, keytab);

        LoginContext loginContext =
            new LoginContext(authType.getAuthName(), subject, null, loginConf);
        loginContext.login();

        Set<KerberosPrincipal> krb5Principals = subject.getPrincipals(KerberosPrincipal.class);
        if (krb5Principals.isEmpty()) {
          throw new LoginException("Kerberos login failed: login subject has no principals.");
        }
        return new User(subject);
      }
      // ENTERPRISE END
      if (authType.equals(AuthType.SIMPLE) || authType.equals(AuthType.CUSTOM)) {
        callbackHandler = new AppLoginModule.AppCallbackHandler(conf);
      }

      // Create LoginContext based on authType, corresponding LoginModule should be registered
      // under the authType name in LoginModuleConfiguration.
      LoginContext loginContext =
          new LoginContext(authType.getAuthName(), subject, callbackHandler,
              new LoginModuleConfiguration());
      loginContext.login();

      Set<User> userSet = subject.getPrincipals(User.class);
      if (userSet.isEmpty()) {
        throw new LoginException("No Alluxio User is found.");
      }
      if (userSet.size() > 1) {
        throw new LoginException("More than one Alluxio User is found");
      }
      return userSet.iterator().next();
    } catch (LoginException e) {
      throw new IOException("Failed to login: " + e.getMessage(), e);
    }
  }

  /**
   * Checks whether Alluxio is running in secure mode, such as {@link AuthType#SIMPLE},
   * {@link AuthType#KERBEROS}, {@link AuthType#CUSTOM}.
   *
   * @param authType the authentication type in configuration
   */
  private static void checkSecurityEnabled(AuthType authType) {
    // ENTERPRISE ADD
    if (authType == AuthType.KERBEROS) {
      return;
    }
    // ENTERPRISE END
    // TODO(dong): add Kerberos condition check.
    if (authType != AuthType.SIMPLE && authType != AuthType.CUSTOM) {
      throw new UnsupportedOperationException("User is not supported in " + authType.getAuthName()
          + " mode");
    }
  }
  // ENTERPRISE ADD

  /**
   * Gets the client login subject if and only if the secure mode is {Authtype#KERBEROS}. Otherwise
   * returns null.
   *
   * @param conf Alluxio configuration
   * @return login Subject if AuthType is KERBEROS, otherwise null
   * @throws IOException if the login failed
   */
  public static Subject getClientLoginSubject(Configuration conf) throws IOException {
    if (conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class) != AuthType.KERBEROS) {
      return null;
    }
    return getClientUser(conf).getSubject();
  }

  /**
   * Gets the server login subject if and only if the secure mode is {Authtype#KERBEROS}. Otherwise
   * returns null.
   *
   * @param conf Alluxio configuration
   * @return login Subject if AuthType is KERBEROS, otherwise null
   * @throws IOException if the login failed
   */
  public static Subject getServerLoginSubject(Configuration conf) throws IOException {
    if (conf.getEnum(Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.class) != AuthType.KERBEROS) {
      return null;
    }
    return getServerUser(conf).getSubject();
  }
  // ENTERPRISE END
}
