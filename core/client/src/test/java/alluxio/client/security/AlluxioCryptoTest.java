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

import alluxio.Constants;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AlluxioCrypto}.
 */
public final class AlluxioCryptoTest {
  private static final String AES_GCM = "AES/GCM/NoPadding";
  private static final String TEST_SECRET_KEY = "yoursecretKey";
  private static final String TEST_IV = "ivvvv";
  private static final int AES_GCM_AUTH_TAG_LENGTH = 16;

  @Test
  public void basic() throws Exception {
    final String[] testcases = {
        "",
        "a",
        "foo",
        "testplaintext",
        new String(new char[64 * Constants.KB]).replace('\0', 'a'),
        new String(new char[4 * Constants.MB]).replace('\0', 'b'),
    };
    CryptoKey key = new CryptoKey(AES_GCM, TEST_SECRET_KEY.getBytes(), TEST_IV.getBytes(), true);

    for (final String plaintext : testcases) {
      AlluxioCrypto crypto = new AlluxioCrypto(AES_GCM);
      byte[] ciphertext = crypto.encrypt(plaintext.getBytes(), key);
      byte[] decrypted = crypto.decrypt(ciphertext, key);
      Assert.assertEquals(plaintext.getBytes().length, ciphertext.length - AES_GCM_AUTH_TAG_LENGTH);
      Assert.assertEquals(plaintext, new String(decrypted));
    }
  }
}
