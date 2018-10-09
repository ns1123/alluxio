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

package alluxio.security.authentication;

import alluxio.security.MasterKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A token used for authentication.
 *
 * @param <T> the identifier of the token which contains the public information
 */
public class Token<T extends TokenIdentifier> {
  public static final Logger LOG = LoggerFactory.getLogger(Token.class);

  private final T mId;
  private final byte[] mPassword;

  /**
   * Constructs a token from identifier and password.
   *
   * @param id token identifier
   * @param password password for the token
   */
  public Token(T id, byte[] password) {
    mId = id;
    mPassword = password.clone();
  }

  /**
   * Constructs a token from identifier and a master key.
   *
   * @param id token identifier
   * @param key master key for generating the password
   */
  public Token(T id, MasterKey key) {
    mId = id;
    mPassword = key.calculateHMAC(id.getBytes());
  }

  /**
   * @return token identifier
   */
  public T getId() {
    return mId;
  }

  /**
   * @return password for the token
   */
  public byte[] getPassword() {
    return mPassword;
  }

  @Override
  public String toString() {
    return String.format("Token(%s)", mId.toString());
  }
}
