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
import alluxio.network.protocol.databuffer.DataNettyBufferV2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CryptoUtils}.
 */
public final class CryptoUtilsTest {
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
      byte[] ciphertext = new byte[plaintext.length() + AES_GCM_AUTH_TAG_LENGTH];
      CryptoUtils.encrypt(key, plaintext.getBytes(), 0, plaintext.length(), ciphertext, 0);
      byte[] decrypted = new byte[plaintext.length()];
      CryptoUtils.decrypt(key, ciphertext, 0, ciphertext.length, decrypted, 0);
      Assert.assertEquals(plaintext.getBytes().length, ciphertext.length - AES_GCM_AUTH_TAG_LENGTH);
      Assert.assertEquals(plaintext, new String(decrypted));
    }
  }

  @Test
  public void byteBuf() throws Exception {
    final String[] testcases = {
        "foo",
        "testplaintext",
        new String(new char[64 * Constants.KB]).replace('\0', 'a'),
        new String(new char[4 * Constants.MB]).replace('\0', 'b'),
    };
    CryptoKey key = new CryptoKey(AES_GCM, TEST_SECRET_KEY.getBytes(), TEST_IV.getBytes(), true);

    for (final String plaintext : testcases) {
      ByteBuf ciphertext = CryptoUtils.encrypt(key, Unpooled.wrappedBuffer(plaintext.getBytes()));
      byte[] decrypted = CryptoUtils.decrypt(key, new DataNettyBufferV2(ciphertext));
      Assert.assertEquals(plaintext.getBytes().length, decrypted.length);
      Assert.assertEquals(plaintext, new String(decrypted));
    }
  }
}
