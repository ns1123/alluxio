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

import alluxio.exception.InvalidCapabilityException;
import alluxio.proto.security.CapabilityProto;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

import org.apache.commons.codec.digest.HmacAlgorithms;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

/**
 * Tests for {@link CapabilityKey}.
 */
public final class CapabilityKeyTest {
  private final long mKeyId = 1L;
  private final long mFileId = 2L;
  private final String mEncodingKey = "mykey";
  private final String mUsername = "testuser";

  private final CapabilityProto.Content mContent = CapabilityProto.Content.newBuilder()
      .setUser(mUsername)
      .setFileId(mFileId)
      .setAccessMode(Mode.Bits.READ.ordinal())
      .setExpirationTimeMs(CommonUtils.getCurrentMs() + 10 * 1000).build();

  private CapabilityKey mKey;

  @Before
  public void before() throws Exception {
    mKey = new CapabilityKey(1L, CommonUtils.getCurrentMs() + 10 * 1000, "mykey".getBytes());
  }

  @Test
  public void defaults() {
    CapabilityKey key = new CapabilityKey();
    Assert.assertEquals(0L, key.getKeyId());
    Assert.assertEquals(0L, key.getExpirationTimeMs());
    Assert.assertNull(key.getEncodedKey());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() throws Exception {
    Random random = new Random();
    long keyid = random.nextLong();
    long expiration = random.nextLong();
    byte[] encodedKey = "test_key".getBytes();

    CapabilityKey key = new CapabilityKey(keyid, expiration, encodedKey);

    Assert.assertEquals(keyid, key.getKeyId());
    Assert.assertEquals(expiration, key.getExpirationTimeMs());
    Assert.assertEquals(new String(encodedKey), new String(key.getEncodedKey()));
  }

  @Test
  public void setNullKey() throws Exception {
    CapabilityKey key = new CapabilityKey(1L, 2L, null);
    Assert.assertNull(key.getEncodedKey());
    try {
      key.calculateHMAC("payload".getBytes());
      Assert.fail("null key can not generate MAC");
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void createWithSecretKey() throws Exception {
    Random random = new Random();
    long keyid = random.nextLong();
    long expiration = random.nextLong();
    KeyGenerator keyGen = KeyGenerator.getInstance(HmacAlgorithms.HMAC_SHA_1.toString());
    keyGen.init(128);
    SecretKey secretKey = keyGen.generateKey();

    CapabilityKey key = new CapabilityKey(keyid, expiration, secretKey.getEncoded());

    Assert.assertEquals(keyid, key.getKeyId());
    Assert.assertEquals(expiration, key.getExpirationTimeMs());
    Assert.assertEquals(16, key.getEncodedKey().length);
    Assert.assertEquals(new String(secretKey.getEncoded()), new String(key.getEncodedKey()));
  }

  @Test
  public void setEmptyKey() throws Exception  {
    try {
      CapabilityKey key = new CapabilityKey(1L, 10000L, "".getBytes());
      Assert.fail("Should get IllegalArgumentException with an empty key.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void caculateHMAC() {
    String expectedHMAC = "5d667a809eb0216e280889096a46d92f2c36db5f";
    Assert.assertEquals(expectedHMAC, FormatUtils.byteArrayToHexString(
        mKey.calculateHMAC("payload".getBytes()), "", ""));

    String expectedHMACForEmptyData = "5bb9c066a336f0e6f17d7ddac4e43de7a94a6c9a";
    Assert.assertEquals(expectedHMACForEmptyData, FormatUtils.byteArrayToHexString(
        mKey.calculateHMAC("".getBytes()), "", ""));
  }

  @Test
  public void generateAndVerifyCapability() throws Exception {
    Capability capability = new Capability(mKey, mContent);
    mKey.verifyAuthenticator(capability);
  }

  @Test
  public void verifyAuthenticatorWithWrongUser() throws Exception {
    alluxio.thrift.Capability capabilityThrift = new Capability(mKey, mContent).toThrift();
    capabilityThrift
        .setContent(mContent.toBuilder().setUser("wronguser").build().toByteArray());
    Capability capability = new Capability(capabilityThrift);
    try {
      mKey.verifyAuthenticator(capability);
      Assert.fail("Changed content should fail to authenticate.");
    } catch (InvalidCapabilityException e) {
      // expected
    }
  }

  @Test
  public void verifyAuthenticatorTestWithNewerKeyId() throws Exception {
    Capability capability = new Capability(mKey, mContent);
    CapabilityKey newerKey = new CapabilityKey(
        mKeyId + 1, CommonUtils.getCurrentMs() + 100 * 1000, mEncodingKey.getBytes());
    try {
      newerKey.verifyAuthenticator(capability);
    } catch (InvalidCapabilityException e) {
      Assert.fail(
          "Verify authenticator with a mismatching key id should not fail if the keys are the "
              + "same.");
    }
  }

  @Test
  public void verifyAuthenticatorTestWithWrongSecretKey() throws Exception {
    CapabilityKey wrongKey = new CapabilityKey(
        mKeyId, CommonUtils.getCurrentMs() + 100 * 1000, "gussedKey".getBytes());
    Capability capability = new Capability(mKey, mContent);
    try {
      wrongKey.verifyAuthenticator(capability);
      Assert.fail("Verify authenticator generated with a wrong secret key should fail.");
    } catch (InvalidCapabilityException e) {
      // expected.
    }
  }
}
