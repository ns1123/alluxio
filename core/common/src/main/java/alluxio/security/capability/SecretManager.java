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

package alluxio.security.capability;

import alluxio.Constants;

import com.google.common.base.Throwables;
import org.apache.commons.codec.digest.HmacAlgorithms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

/**
 * Secret manager generates secret keys for capabilities.
 */
@ThreadSafe
public final class SecretManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String HMAC_ALGORITHM = HmacAlgorithms.HMAC_SHA_1.toString();
  private static final int KEY_LENGTH = 128;

  @GuardedBy("this")
  private KeyGenerator mKeyGen;

  /**
   * Creates a new {@link SecretManager}.
   */
  public SecretManager() {
    try {
      mKeyGen = KeyGenerator.getInstance(HMAC_ALGORITHM);
      mKeyGen.init(KEY_LENGTH);
    } catch (NoSuchAlgorithmException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Generates a new secret key.
   *
   * @return SecretKey the generated secret key
   */
  public synchronized SecretKey generateSecret() {
    return mKeyGen.generateKey();
  }
}
