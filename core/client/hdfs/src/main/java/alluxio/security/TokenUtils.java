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

import alluxio.conf.AlluxioConfiguration;
import alluxio.hadoop.AlluxioDelegationTokenIdentifier;
import alluxio.security.authentication.DelegationTokenIdentifier;
import alluxio.security.authentication.Token;
import alluxio.util.ConfigurationUtils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * Utility class for using Alluxio tokens with the Hadoop client.
 */
@ThreadSafe
public final class TokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TokenUtils.class);
  private static final AbstractDelegationTokenSelector<AlluxioDelegationTokenIdentifier>
      SELECTOR =
      new AbstractDelegationTokenSelector<AlluxioDelegationTokenIdentifier>(
          AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND) {
      };

  /**
   * @param uri the Alluxio service uri
   * @param conf the Alluxio configuration
   * @return the token service name
   */
  public static String buildTokenService(URI uri, AlluxioConfiguration conf) {
    if (ConfigurationUtils.isHaMode(conf)) {
      // builds token service name for logic alluxio service uri (HA)
      return uri.toString();
    }

    // builds token service name for single master address.
    return SecurityUtil.buildTokenService(uri).toString();
  }

  /**
   * @param ugi the hadoop UGI
   * @return true if the UGI contains Alluxio tokens
   */
  public static boolean hasAlluxioTokens(UserGroupInformation ugi) {
    return ugi.getTokens().stream().anyMatch(
        x -> AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND.equals(x.getKind()));
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
    Collection<org.apache.hadoop.security.token.Token<? extends TokenIdentifier>> tokens =
        ugi.getTokens();
    if (tokens.isEmpty()) {
      LOG.debug("No tokens found for {}.", ugi);
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("retrieved HDFS tokens from UGI: {}", Arrays.toString(tokens.toArray()));
    }
    org.apache.hadoop.security.token.Token<AlluxioDelegationTokenIdentifier> token =
        SELECTOR.selectToken(new Text(serviceName), tokens);
    if (token != null) {
      LOG.debug("retrieved Alluxio token from UGI for service {}: {}", serviceName, token);

      DelegationTokenIdentifier id = token.decodeIdentifier().getAlluxioIdentifier();
      Token<DelegationTokenIdentifier> alluxioToken = new Token<>(id, token.getPassword());
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

  private static void addCredentialsToSubject(String name, Token token, Subject subject) {
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
      LOG.debug("Added Alluxio token to subject. token: {} subject: {}", token.toString(),
          subject.toString());
    } else {
      LOG.debug("no Alluxio subject/token found.");
    }
  }

  private TokenUtils() {} // prevent instantiation
}
