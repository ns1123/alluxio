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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Secret manager maintains the secret key for capabilities. It is used to generate the
 * authenticator(HMAC) for a byte array, with the secret key.
 */
public final class SecretManager {
  // TODO(chaomin): add the SecretKey generation logic.
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";
  private static final int MIN_SECRET_KEY_SIZE = 16;

  /**
   * Calculates the HMAC SHA1 with given data and key.
   *
   * @param key the secret key in bytes
   * @param data the data content to calculate HMAC
   * @return the HMAC result in bytes
   */
  public static byte[] calculateHMAC(byte[] key, byte[] data) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(data);
    // TODO(chaomin): Fail directly.
    if (key.length < MIN_SECRET_KEY_SIZE) {
      LOG.error("Secret key too short");
    }
    SecretKeySpec signingKey = new SecretKeySpec(key, HMAC_SHA1_ALGORITHM);
    try {
      Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
      mac.init(signingKey);
      return mac.doFinal(data);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw Throwables.propagate(e);
    }
  }

  private SecretManager() {} // prevent instantiation
}
