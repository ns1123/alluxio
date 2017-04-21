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

package alluxio.client.security;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CryptoKey}.
 */
public final class CryptoKeyTest {
  @Test
  public void basic() throws Exception {
    final String cipherName = "AES/GCM/NoPadding";
    final String secretKey = "yoursecretKey";
    final String iv = "iv";
    final boolean needAuthTag = false;
    CryptoKey key = new CryptoKey(cipherName, secretKey.getBytes(), iv.getBytes(), needAuthTag);

    Assert.assertEquals(cipherName, key.getCipher());
    Assert.assertEquals(secretKey, new String(key.getKey()));
    Assert.assertEquals(iv, new String(key.getIv()));
    Assert.assertEquals(needAuthTag, key.isNeedsAuthTag());
  }

  @Test
  public void authTag() throws Exception {
    final String cipherName = "AES/GCM/NoPadding";
    final String secretKey = "anothersecretKey";
    final String iv = "iviviv";
    final boolean needAuthTag = true;
    final String authTag = "*ThisIsAuthTag!";
    CryptoKey key = new CryptoKey(
        cipherName, secretKey.getBytes(), iv.getBytes(), needAuthTag, authTag.getBytes());

    Assert.assertEquals(cipherName, key.getCipher());
    Assert.assertEquals(secretKey, new String(key.getKey()));
    Assert.assertEquals(iv, new String(key.getIv()));
    Assert.assertEquals(needAuthTag, key.isNeedsAuthTag());
    Assert.assertEquals(authTag, new String(key.getAuthTag()));
  }
}
