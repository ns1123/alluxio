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

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.util.EncryptionMetaTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.proto.security.EncryptionProto;
import alluxio.util.proto.ProtoUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CryptoUtils} with {@link JavaCipher}.
 */
public class CryptoUtilsTest {
  private static final String AES_GCM = Constants.AES_GCM_NOPADDING;
  private static final String TEST_SECRET_KEY = "thisis16byteskey";
  private static final String TEST_IV = "ivvvv";
  private static final int AES_GCM_AUTH_TAG_LENGTH = 16;
  private EncryptionProto.CryptoKey mKey =
      ProtoUtils.setIv(
        ProtoUtils.setKey(
          EncryptionProto.CryptoKey.newBuilder()
          .setCipher(AES_GCM)
          .setNeedsAuthTag(1)
          .setGenerationId("generationBytes"), TEST_SECRET_KEY.getBytes()),
      TEST_IV.getBytes()).build();

  protected InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

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

    for (final String plaintext : testcases) {
      byte[] ciphertext = new byte[plaintext.length() + AES_GCM_AUTH_TAG_LENGTH];
      CryptoUtils.encrypt(mKey, plaintext.getBytes(), 0, plaintext.length(), ciphertext, 0, mConf);
      byte[] decrypted = new byte[plaintext.length()];
      CryptoUtils.decrypt(mKey, ciphertext, 0, ciphertext.length, decrypted, 0, mConf);
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
    EncryptionProto.Meta meta = EncryptionMetaTestUtils.create(mConf);

    for (final String plaintext : testcases) {
      ByteBuf ciphertext = CryptoUtils.encryptChunks(
          meta, Unpooled.wrappedBuffer(plaintext.getBytes()), mConf);
      ByteBuf decrypted = CryptoUtils.decryptChunks(meta, ciphertext, mConf);
      Assert.assertEquals(plaintext.getBytes().length, decrypted.readableBytes());
      Assert.assertEquals(plaintext, new String(decrypted.array()));
    }
  }
}
