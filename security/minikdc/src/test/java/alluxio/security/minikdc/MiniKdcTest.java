/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.security.minikdc;

import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

public final class MiniKdcTest extends KerberosSecurityTestcase {
  private static final boolean IBM_JAVA = System.getProperty("java.vendor").contains("IBM");

  /**
   * Test starting a miniKDC.
   */
  @Test
  public void miniKdcStartTest() {
    MiniKdc kdc = getKdc();
    Assert.assertNotSame(0, kdc.getPort());
  }

  /**
   * Test generating a keytab.
   */
  @Test
  public void keytabGenTest() throws Exception {
    MiniKdc kdc = getKdc();
    File workDir = getWorkDir();

    kdc.createPrincipal(new File(workDir, "keytab"), "foo/bar", "bar/foo");
    Keytab kt = Keytab.read(new File(workDir, "keytab"));
    Set<String> principals = new HashSet<String>();
    for (KeytabEntry entry : kt.getEntries()) {
      principals.add(entry.getPrincipalName());
    }
    // here principals use \ instead of /
    // because org.apache.directory.server.kerberos.shared.keytab.KeytabDecoder
    // .getPrincipalName(IoBuffer buffer) use \\ when generates principal
    Assert
        .assertEquals(
            new HashSet<String>(
                Arrays.asList("foo\\bar@" + kdc.getRealm(), "bar\\foo@" + kdc.getRealm())),
            principals);
  }

  private static final class KerberosConfiguration extends Configuration {
    private String mPrincipal;
    private String mKeytab;
    private boolean mIsInitiator;

    private KerberosConfiguration(String principal, File keytab, boolean client) {
      mPrincipal = principal;
      mKeytab = keytab.getAbsolutePath();
      mIsInitiator = client;
    }

    public static Configuration createClientConfig(String principal, File keytab) {
      return new KerberosConfiguration(principal, keytab, true);
    }

    public static Configuration createServerConfig(String principal, File keytab) {
      return new KerberosConfiguration(principal, keytab, false);
    }

    private static String getKrb5LoginModuleName() {
      return System.getProperty("java.vendor").contains("IBM")
          ? "com.ibm.security.auth.module.Krb5LoginModule"
          : "com.sun.security.auth.module.Krb5LoginModule";
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();
      options.put("principal", mPrincipal);
      options.put("refreshKrb5Config", "true");
      if (IBM_JAVA) {
        options.put("useKeytab", mKeytab);
        options.put("credsType", "both");
      } else {
        options.put("keyTab", mKeytab);
        options.put("useKeyTab", "true");
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("useTicketCache", "true");
        options.put("renewTGT", "true");
        options.put("isInitiator", Boolean.toString(mIsInitiator));
      }
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[] {new AppConfigurationEntry(getKrb5LoginModuleName(),
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
    }
  }

  /**
   * Test Kerberos login for both client and server.
   */
  @Test
  public void kerberosLoginTest() throws Exception {
    MiniKdc kdc = getKdc();
    File workDir = getWorkDir();
    LoginContext loginContext = null;
    try {
      String principal = "foo";
      File keytab = new File(workDir, "foo.keytab");
      kdc.createPrincipal(keytab, principal);

      Set<Principal> principals = new HashSet<Principal>();
      principals.add(new KerberosPrincipal(principal));

      // client login
      Subject subject =
          new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
      loginContext = new LoginContext("", subject, null,
          KerberosConfiguration.createClientConfig(principal, keytab));
      loginContext.login();
      subject = loginContext.getSubject();
      Assert.assertEquals(1, subject.getPrincipals().size());
      Assert.assertEquals(KerberosPrincipal.class,
          subject.getPrincipals().iterator().next().getClass());
      Assert.assertEquals(principal + "@" + kdc.getRealm(),
          subject.getPrincipals().iterator().next().getName());
      loginContext.logout();

      // server login
      subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
      loginContext = new LoginContext("", subject, null,
          KerberosConfiguration.createServerConfig(principal, keytab));
      loginContext.login();
      subject = loginContext.getSubject();
      Assert.assertEquals(1, subject.getPrincipals().size());
      Assert.assertEquals(KerberosPrincipal.class,
          subject.getPrincipals().iterator().next().getClass());
      Assert.assertEquals(principal + "@" + kdc.getRealm(),
          subject.getPrincipals().iterator().next().getName());
      loginContext.logout();

    } finally {
      if (loginContext != null) {
        loginContext.logout();
      }
    }
  }
}
