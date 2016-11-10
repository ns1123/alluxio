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

import com.google.common.base.Preconditions;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Formatter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Secret manager maintains the secret key for capabilities. It is used to generate the
 * authenticator(HMAC) for a given {@link CapabilityContent}, with the secret key.
 */
public final class SecretManager {
  // TODO(chaomin): add the SecretKey generation logic.
  private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

  /**
   * Calculates the HMAC SHA1 with given data and key.
   *
   * @param key the key
   * @param data the data content to calculate HMAC
   * @return the HMAC result
   * @throws SignatureException if failed to create HMAC
   * @throws NoSuchAlgorithmException if the algorithm is invalid
   * @throws InvalidKeyException if the key is invalid
   */
  public static String calculateHMAC(String key, String data)
      throws SignatureException, NoSuchAlgorithmException, InvalidKeyException {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(key);
    SecretKeySpec signingKey = new SecretKeySpec(key.getBytes(), HMAC_SHA1_ALGORITHM);
    Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
    mac.init(signingKey);
    return toHexString(mac.doFinal(data.getBytes()));
  }

  /**
   * Convert bytes to hex in string.
   *
   * @param bytes the input in bytes
   * @return the result hex string
   */
  public static String toHexString(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    Formatter formatter = new Formatter();
    for (byte b : bytes) {
      formatter.format("%02x", b);
    }
    return formatter.toString();
  }

  private SecretManager() {} // prevent instantiation
}
