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
 * Unit tests for {@link AlluxioCrypto}.
 */
public final class AlluxioCryptoTest {
  @Test
  public void basic() throws Exception {
    final String cipher = "AES/GCM/NoPadding";
    CryptoKey encryptKey = new CryptoKey(
        cipher, "yoursecrectKey".getBytes(), "ivvvv".getBytes(), true);
    CryptoKey decryptKey = new CryptoKey(
        cipher, "yoursecrectKey".getBytes(), "ivvvv".getBytes(), true);
    AlluxioCrypto crypto = new AlluxioCrypto(cipher);
    byte[] ciphertext = crypto.encrypt("testinputtttttt".getBytes(), encryptKey);
    byte[] decrypted = crypto.decrypt(ciphertext, decryptKey);
    Assert.assertEquals("testinputtttttt".getBytes().length, ciphertext.length - 16);
    Assert.assertEquals("testinputtttttt", new String(decrypted));
  }

  @Test
  public void smallinput() throws Exception {
    final String cipher = "AES/GCM/NoPadding";
    CryptoKey encryptKey = new CryptoKey(
        cipher, "yoursecrectKey".getBytes(), "ivvvv".getBytes(), true);
    CryptoKey decryptKey = new CryptoKey(
        cipher, "yoursecrectKey".getBytes(), "ivvvv".getBytes(), true);
    AlluxioCrypto crypto = new AlluxioCrypto(cipher);
    byte[] ciphertext = crypto.encrypt("t".getBytes(), encryptKey);
    byte[] decrypted = crypto.decrypt(ciphertext, decryptKey);
    Assert.assertEquals("t".getBytes().length, ciphertext.length - 16);
    Assert.assertEquals("t", new String(decrypted));
  }
}
