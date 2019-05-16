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

package alluxio.security.login;

import alluxio.security.authentication.AuthType;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

/**
 * A JAAS configuration for kerberos that defines the login modules, by which JAAS uses to login.
 */
@ThreadSafe
public final class KerberosLoginConfiguration extends Configuration {
  /** The Kerberos principal in string format for login. */
  private String mPrincipal;
  /** The Kerberos Keytab file path containing the principal credentials. */
  private String mKeytab;

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
      // TODO(chaomin): maybe add "isInitiator".
    }
  };

  /**
   * Constructor for Kerberos {@link KerberosLoginConfiguration}.
   *
   * @param principal Kerberos principal name
   * @param keytab Kerberos keytab file absolute path
   */
  public KerberosLoginConfiguration(String principal, String keytab) {
    mPrincipal = principal;
    mKeytab = keytab;
  }

  /**
   * Constructs a new {@link KerberosLoginConfiguration}.
   */
  public KerberosLoginConfiguration() {}

  @Override
  @Nullable
  public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
    if (!appName.equalsIgnoreCase(AuthType.KERBEROS.getAuthName())) {
      return null;
    }
    // Kerberos login option 1: login from keytab file if the given principal and keytab files
    // are valid.
    Map<String, String> keytabOptions = new HashMap<String, String>();
    keytabOptions.putAll(KERBEROS_OPTIONS);
    keytabOptions.put("useKeyTab", "true");
    keytabOptions.put("useTicketCache", "false");
    keytabOptions.put("renewTGT", "false");
    keytabOptions.put("storeKey", "true");
    keytabOptions.put("keyTab", mKeytab);
    keytabOptions.put("principal", mPrincipal);
    AppConfigurationEntry kerberosLoginFromKeytab =
        new AppConfigurationEntry(alluxio.security.util.KerberosUtils.getKrb5LoginModuleName(),
            LoginModuleControlFlag.OPTIONAL,
            keytabOptions);

    // Kerberos login option 2: login from Kerberos ticket cache if kinit is run on the machine.
    // This would happen if the option 1 Keytab login failed.
    Map<String, String> ticketCacheOptions = new HashMap<String, String>();
    ticketCacheOptions.putAll(KERBEROS_OPTIONS);
    ticketCacheOptions.put("useKeyTab", "false");
    AppConfigurationEntry kerberosLoginFromTicketCache =
        new AppConfigurationEntry(alluxio.security.util.KerberosUtils.getKrb5LoginModuleName(),
            LoginModuleControlFlag.OPTIONAL,
            ticketCacheOptions);

    return new AppConfigurationEntry[]{ kerberosLoginFromKeytab, kerberosLoginFromTicketCache };
  }
}
