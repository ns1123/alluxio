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
import alluxio.security.util.KerberosUtil;
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
  private String mPrincipal;
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

  // TODO(dong): add Kerberos_LOGIN module
  // private static final AppConfigurationEntry KERBEROS_LOGIN = ...

  /**
   * In the {@link AuthType#SIMPLE} mode, JAAS first tries to retrieve the user name set by the
   * application with {@link AppLoginModule}. Upon failure, it uses the OS specific login module to
   * fetch the OS user, and then uses {@link AlluxioLoginModule} to convert it to an Alluxio user
   * represented by {@link User}. In {@link AuthType#CUSTOM} mode, we also use this configuration.
   */
  private static final AppConfigurationEntry[] SIMPLE =
      new AppConfigurationEntry[] {APP_LOGIN, OS_SPECIFIC_LOGIN, ALLUXIO_LOGIN};

  // TODO(dong): add Kerberos mode
  // private static final AppConfigurationEntry[] KERBEROS = ...

  // ENTERPRISE ADD
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
  // ENTERPRISE END

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
    if (appName.equalsIgnoreCase(AuthType.SIMPLE.getAuthName())
        || appName.equalsIgnoreCase(AuthType.CUSTOM.getAuthName())) {
      return SIMPLE;
    } else if (appName.equalsIgnoreCase(AuthType.KERBEROS.getAuthName())) {
      // ENTERPRISE EDIT
      Map<String, String> options = new HashMap<String, String>();
      options.put("keyTab", mKeytab);
      options.put("principal", mPrincipal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      // TODO(chaomin): maybe add "isInitiator".
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }

      return new AppConfigurationEntry[]{
          new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
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
