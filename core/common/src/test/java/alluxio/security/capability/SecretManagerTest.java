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

import org.apache.commons.codec.digest.HmacAlgorithms;
import org.junit.Assert;
import org.junit.Test;

import javax.crypto.SecretKey;

/**
 * Unit tests for {@link SecretManager}.
 */
public final class SecretManagerTest {
  @Test
  public void basic() {
    SecretManager sm = new SecretManager();
    SecretKey secretKey = sm.generateSecret();
    Assert.assertEquals(HmacAlgorithms.HMAC_SHA_1.toString(), secretKey.getAlgorithm());
    Assert.assertEquals(16, secretKey.getEncoded().length);
  }

  @Test
  public void multipleKeysAreNotEqual() {
    SecretManager sm = new SecretManager();
    SecretKey key1 = sm.generateSecret();
    SecretKey key2 = sm.generateSecret();
    Assert.assertNotEquals(key1, key2);
  }
}
