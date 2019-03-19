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

package alluxio.security.authentication.token;

import alluxio.security.authentication.Token;
import alluxio.security.capability.CapabilityToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.util.Iterator;

/**
 * Utils for token schemes.
 */
public class TokenUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TokenUtils.class);

  public static final String TOKEN_PROTOCOL_NAME = "AlluxioEnterpriseToken";
  public static final String DIGEST_MECHANISM_NAME = "DIGEST-MD5";

  /**
   * Looks for a capability token in the given subject and returns the first one.
   * @param subject the subject
   * @return {@code null} if no capability token found
   */
  public static Token<CapabilityToken.CapabilityTokenIdentifier> getCapabilityTokenFromSubject(
      Subject subject) {
    java.util.Set<Token> subjectTokens = subject.getPrivateCredentials(Token.class);
    if (subjectTokens.size() == 0) {
      LOG.debug("Could not find any token in subject: {}", subject);
      return null;
    }

    Iterator<Token> tokenIter = subjectTokens.iterator();
    while (tokenIter.hasNext()) {
      Token<?> token = tokenIter.next();
      if (token.getId() instanceof CapabilityToken.CapabilityTokenIdentifier) {
        return (Token<CapabilityToken.CapabilityTokenIdentifier>) token;
      }
    }
    LOG.debug("Could not find capability token in subject: {}", subject);
    return null;
  }
}
