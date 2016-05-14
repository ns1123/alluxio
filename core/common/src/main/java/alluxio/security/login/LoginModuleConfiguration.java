/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.login;

import alluxio.security.User;
import alluxio.security.authentication.AuthType;
// ENTERPRISE ADD
import alluxio.security.util.KerberosUtils;
// ENTERPRISE END

import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

/**
 * A JAAS configuration that defines the login modules, by which JAAS uses to login.
 *
 * In implementation, we define several modes (Simple, Kerberos, ...) by constructing different
 * arrays of AppConfigurationEntry, and select the proper array based on the configured mode.
 *
 * Then JAAS login framework use the selected array of AppConfigurationEntry to determine the login
 * modules to be used.
 */
@ThreadSafe
public final class LoginModuleConfiguration extends Configuration {
  // ENTERPRISE ADD
  /** The Kerberos principal in string format for login. */
  private String mPrincipal;
  /** The Kerberos Keytab file path containing the principal credentials. */
  private String mKeytab;
  // ENTERPRISE END

  private static final Map<String, String> EMPTY_JAAS_OPTIONS = new HashMap<String, String>();

  /** Login module that allows a user name provided by OS. */
  private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
      new AppConfigurationEntry(LoginModuleConfigurationUtils.OS_LOGIN_MODULE_NAME,
          LoginModuleControlFlag.REQUIRED, EMPTY_JAAS_OPTIONS);

  /** Login module that allows a user name provided by application to be specified. */
  private static final AppConfigurationEntry APP_LOGIN = new AppConfigurationEntry(
      AppLoginModule.class.getName(), LoginModuleControlFlag.SUFFICIENT, EMPTY_JAAS_OPTIONS);

  /** Login module that allows a user name provided by an Alluxio specific login module. */
  private static final AppConfigurationEntry ALLUXIO_LOGIN = new AppConfigurationEntry(
      AlluxioLoginModule.class.getName(), LoginModuleControlFlag.REQUIRED, EMPTY_JAAS_OPTIONS);

  // ENTERPRISE EDIT
  private static final Map<String, String> KERBEROS_OPTIONS = new HashMap<String, String>() {
    {
      if (System.getProperty("java.vendor").contains("IBM")) {
        put("useDefaultCcache", "true");
      } else {
        put("doNotPrompt", "true");
        put("useTicketCache", "true");
      }
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        if (System.getProperty("java.vendor").contains("IBM")) {
          // The first value searched when "useDefaultCcache" is used.
          System.setProperty("KRB5CCNAME", ticketCache);
        } else {
          put("ticketCache", ticketCache);
        }
      }
      put("renewTGT", "true");
      put("useKeyTab", "true");
      put("storeKey", "true");
      // TODO(chaomin): maybe add "isInitiator".
    }
  };
  // ENTERPRISE REPLACES
  // // TODO(dong): add Kerberos_LOGIN module
  // // private static final AppConfigurationEntry KERBEROS_LOGIN = ...
  // ENTERPRISE END

  /**
   * In the {@link AuthType#SIMPLE} mode, JAAS first tries to retrieve the user name set by the
   * application with {@link AppLoginModule}. Upon failure, it uses the OS specific login module to
   * fetch the OS user, and then uses {@link AlluxioLoginModule} to convert it to an Alluxio user
   * represented by {@link User}. In {@link AuthType#CUSTOM} mode, we also use this configuration.
   */
  private static final AppConfigurationEntry[] SIMPLE =
      new AppConfigurationEntry[] {APP_LOGIN, OS_SPECIFIC_LOGIN, ALLUXIO_LOGIN};

  // ENTERPRISE EDIT
  /**
   * Default constructor.
   */
  public LoginModuleConfiguration() {}

  /**
   * Constructor for Kerberos {@link LoginModuleConfiguration}.
   *
   * @param principal Kerberos principal name
   * @param keytab Kerberos keytab file absolute path
   */
  public LoginModuleConfiguration(String principal, String keytab) {
    mPrincipal = principal;
    mKeytab = keytab;
  }
  // ENTERPRISE REPLACES
  // // TODO(dong): add Kerberos mode
  // // private static final AppConfigurationEntry[] KERBEROS = ...
  // ENTERPRISE END

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
    if (appName.equalsIgnoreCase(AuthType.SIMPLE.getAuthName())
        || appName.equalsIgnoreCase(AuthType.CUSTOM.getAuthName())) {
      return SIMPLE;
    } else if (appName.equalsIgnoreCase(AuthType.KERBEROS.getAuthName())) {
      // ENTERPRISE EDIT
      Map<String, String> options = KERBEROS_OPTIONS;
      options.put("keyTab", mKeytab);
      options.put("principal", mPrincipal);

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(KerberosUtils.getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
              options)
      };
      // ENTERPRISE REPLACES
      // // TODO(dong): return KERBEROS;
      // throw new UnsupportedOperationException("Kerberos is not supported currently.");
      // ENTERPRISE END
    }
    return null;
  }
}
