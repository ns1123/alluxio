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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Secret manager maintains the secret key for capabilities. It is used to generate the
 * authenticator(HMAC) for a given {@link CapabilityContent}, with the secret key.
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
   * @throws SignatureException if failed to create HMAC
   * @throws NoSuchAlgorithmException if the algorithm is invalid
   * @throws InvalidKeyException if the key is invalid
   */
  public static byte[] calculateHMAC(byte[] key, String data)
      throws SignatureException, NoSuchAlgorithmException, InvalidKeyException {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(data);
    if (key.length < MIN_SECRET_KEY_SIZE) {
      LOG.error("Secret key too short");
    }
    SecretKeySpec signingKey = new SecretKeySpec(key, HMAC_SHA1_ALGORITHM);
    Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
    mac.init(signingKey);
    return mac.doFinal(data.getBytes());
  }

  private SecretManager() {} // prevent instantiation
}
