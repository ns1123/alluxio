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

import alluxio.security.authentication.Token;
import alluxio.security.authentication.TokenIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stores Alluxio credentials.
 */
public class Credentials {
  private static final Logger LOG = LoggerFactory.getLogger(Credentials.class);

  private Map<String, Token> mTokens = new HashMap<>();

  /**
   * Adds a token to credentials.
   *
   * @param name name of the token
   * @param token the token to add
   */
  public void addToken(String name, Token token) {
    mTokens.put(name, token);
  }

  /**
   * Gets a token from credentials.
   *
   * @param name name of the token
   * @return a token with the given name
   */
  public Token getToken(String name) {
    return mTokens.get(name);
  }

  /**
   * Gets all tokens of a specific identifier type in the credentials.
   *
   * @param <T> token identifier type
   * @param idClass type of the token identifier
   * @return a collection of the tokens
   */
  public <T extends TokenIdentifier> Collection<Token<T>> allTokens(Class<T> idClass) {
    return mTokens.values().stream().filter(token -> idClass.isInstance(token.getId()))
        .map(token -> (Token<T>) token).collect(Collectors.toList());
  }
}
