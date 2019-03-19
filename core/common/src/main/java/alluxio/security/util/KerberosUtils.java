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
import alluxio.security.Credentials;
import alluxio.security.authentication.DelegationTokenIdentifier;
import alluxio.security.authentication.Token;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;

/**
 * Utils for Kerberos.
 */
public final class KerberosUtils {
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
    LOG.debug("getting delegation tokens for subject: {}", subject);
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

  private KerberosUtils() {} // prevent instantiation
}
