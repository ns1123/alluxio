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

package alluxio.hadoop;

import alluxio.conf.AlluxioConfiguration;
import alluxio.security.Credentials;
import alluxio.security.User;
import alluxio.security.authentication.DelegationTokenIdentifier;
import alluxio.security.authentication.KerberosLoginProvider;
import alluxio.security.util.KerberosUtils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginException;

/**
 * Provides Kerberos authentication using Hadoop authentication APIs.
 */
public class HadoopKerberosLoginProvider implements KerberosLoginProvider {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopKerberosLoginProvider.class);
  private static final AbstractDelegationTokenSelector<AlluxioDelegationTokenIdentifier> SELECTOR =
      new AbstractDelegationTokenSelector<AlluxioDelegationTokenIdentifier>(
          AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND
      ) {};

  @Override
  public Subject login() throws LoginException {
    LOG.info("Login using HadoopKerberosLoginProvider");
    Subject subject = null;
    UserGroupInformation ugi = null;
    try {
      // Checks current user for delegation tokens. Delegation tokens are always stored in current user.
      ugi = UserGroupInformation.getCurrentUser();
      if (hasAlluxioDelegationTokens(ugi)) {
        subject = getSubjectFromUGI(ugi);
      }
      if (subject == null) {
        // Checks login user for Kerberos credentials. If Hadoop is authenticated using Kerberos,
        // the credentials are stored in login user.
        ugi = UserGroupInformation.getLoginUser();
        if (ugi.hasKerberosCredentials()) {
          subject = getSubjectFromUGI(ugi);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception occurred while login with Hadoop user: ", e);
      throw new LoginException(String.format("Failed to login with Hadoop user: %s",
          e.getMessage()));
    }
    if (subject == null) {
      throw new LoginException("could not retrieve subject from UserGroupInformation");
    }
    if (subject.getPrincipals(User.class).isEmpty()) {
      subject.getPrincipals().add(new User(ugi.getShortUserName()));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("UGI tokens: {}", Arrays.toString(ugi.getTokens().toArray()));
    }
    if (!hasAlluxioDelegationTokens(ugi)) {
      KerberosTicket tgt = KerberosUtils.extractOriginalTGTFromSubject(subject);
      if (tgt == null) {
        throw new LoginException("could not retrieve TGT from subject");
      }
    }
    return subject;
  }

  @Override
  public void relogin() throws LoginException {
    try {
      if (UserGroupInformation.isLoginKeytabBased()) {
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      }
    } catch (IOException e) {
      throw new LoginException(String.format("Failed to relogin using hadoop user: %s.",
          e.getMessage()));
    }
  }

  @Override
  public boolean hasKerberosCredentials() {
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      boolean hasTokens = hasAlluxioDelegationTokens(currentUser);
      if (LOG.isDebugEnabled()) {
        LOG.debug("checking Kerberos credentials - current user: {}", currentUser.toString());
        LOG.debug("current user tokens: {}", Arrays.toString(currentUser.getTokens().toArray()));
        LOG.debug("current user has Alluxio tokens: {}", hasTokens);
      }
      if (hasTokens) {
        // avoids additional login if we already have delegation tokens to login
        return true;
      }
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      if (LOG.isDebugEnabled()) {
        LOG.debug("checking Kerberos credentials - login user: {}", loginUser.toString());
        LOG.debug("login user UGI tokens: {}", Arrays.toString(loginUser.getTokens().toArray()));
        LOG.debug("login user has Kerberos Credentials: {}", loginUser.hasKerberosCredentials());
      }
      return loginUser.hasKerberosCredentials();
    } catch (IOException e) {
      LOG.debug("could not log in using UserGroupInformation {}", e.getMessage());
      return false;
    }
  }

  private Subject getSubjectFromUGI(UserGroupInformation ugi) throws IOException, InterruptedException {
    return ugi.doAs(
        (PrivilegedExceptionAction<Subject>) () -> {
          AccessControlContext context =
              AccessController.getContext();
          return Subject.getSubject(context);
        });
  }

  private static boolean hasAlluxioDelegationTokens(UserGroupInformation ugi) {
    return ugi.getTokens().stream().anyMatch(
        x -> AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND.equals(x.getKind()));
  }

  private static void addCredentialsToSubject(String name,
      alluxio.security.authentication.Token token, Subject subject) {
    if (subject != null && token != null) {
      synchronized (subject) {
        Set<Credentials> allCredentials = subject.getPrivateCredentials(Credentials.class);
        Credentials credentials = null;
        if (allCredentials.isEmpty()) {
          credentials = new Credentials();
          subject.getPrivateCredentials().add(credentials);
        } else {
          credentials = allCredentials.iterator().next();
        }
        credentials.addToken(name, token);
      }
      LOG.debug("set Alluxio token to subject: {}", token.toString());
      LOG.debug("new subject with token: {}", subject.toString());
    } else {
      LOG.debug("no Alluxio subject/token found.");
    }
  }

  /**
   * Populates Alluxio delegation tokens to subject based on Hadoop UGI.
   *
   * @param ugi Hadoop UserGroupInformation that contains delegation tokens
   * @param subject subject where the delegation token should be populated to
   * @param serviceName name of Alluxio service for which delegation tokens should be processed
   * @param masterAddresses addresses for masters
   * @param alluxioConf Alluxio configuration
   * @return whether delegation token for the services are detected and populated
   */
  public static boolean populateAlluxioTokens(UserGroupInformation ugi, Subject subject,
      String serviceName, List<String> masterAddresses, AlluxioConfiguration alluxioConf)
      throws IOException {
    LOG.debug("retrieving tokens from UGI: {}", ugi);
    Collection<Token<? extends TokenIdentifier>> tokens = ugi.getTokens();
    if (tokens.isEmpty()) {
      LOG.debug("No tokens found for {}.", ugi);
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("retrieved HDFS tokens from UGI: {}", Arrays.toString(tokens.toArray()));
    }
    Token<AlluxioDelegationTokenIdentifier> token = SELECTOR.selectToken(new Text(serviceName), tokens);
    if (token != null) {
      LOG.debug("retrieved Alluxio token from UGI for service {}: {}", serviceName, token);

      DelegationTokenIdentifier id = token.decodeIdentifier().getAlluxioIdentifier();
      alluxio.security.authentication.Token<DelegationTokenIdentifier> alluxioToken =
          new alluxio.security.authentication.Token<>(id, token.getPassword());
      LOG.debug("adding tokens for subject: {}", subject);
      addCredentialsToSubject(serviceName, alluxioToken, subject);
      for (String masterAddress : masterAddresses) {
        addCredentialsToSubject(masterAddress, alluxioToken, subject);
      }
      return true;
    }
    LOG.debug("No Alluxio token found in UGI for service {}", serviceName);
    return false;
  }
}
