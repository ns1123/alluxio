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
import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CapabilityUtils}.
 */
public final class CapabilityUtilsTest {
  private final long mKeyId = 1L;
  private final long mFileId = 2L;
  private final String mEncodingKey = "mykey";
  private final String mUsername = "testuser";

  private final CapabilityKey mKey = CapabilityKey.defaults()
      .setKeyId(mKeyId)
      .setEncodedKey(mEncodingKey.getBytes())
      .setExpirationTimeMs(CommonUtils.getCurrentMs() + 100 * 1000);

  private final CapabilityContent mReadContent = CapabilityContent.defaults()
      .setOwner(mUsername)
      .setKeyId(mKeyId)
      .setFileId(mFileId)
      .setAccessMode(CapabilityContent.AccessMode.READ)
      .setExpirationTimeMs(CommonUtils.getCurrentMs() + 10 * 1000);

  private final CapabilityContent mWriteContent = CapabilityContent.defaults()
      .setOwner(mUsername)
      .setKeyId(mKeyId)
      .setFileId(mFileId)
      .setAccessMode(CapabilityContent.AccessMode.WRITE)
      .setExpirationTimeMs(CommonUtils.getCurrentMs() + 10 * 1000);

  @Test
  public void verifyAuthenticatorTest() throws Exception {
    Capability capability = CapabilityUtils.generateCapability(mKey, mReadContent);
    Assert.assertTrue(CapabilityUtils.verifyAuthenticator(mKey, capability));
  }

  @Test
  public void verifyAuthenticatorTestWithNewerKeyId() throws Exception {
    Capability capability = CapabilityUtils.generateCapability(mKey, mReadContent);
    CapabilityKey newerKey = CapabilityKey.defaults()
        .setKeyId(mKeyId + 1)
        .setEncodedKey(mEncodingKey.getBytes())
        .setExpirationTimeMs(CommonUtils.getCurrentMs() + 100 * 1000);
    try {
      CapabilityUtils.verifyAuthenticator(newerKey, capability);
      Assert.fail("Verify authenticator with a mismatching key id should fail.");
    } catch (InvalidCapabilityException e) {
      // expected.
    }
  }

  @Test
  public void verifyAuthenticatorTestWithWrongSecretKey() throws Exception {
    CapabilityKey wrongKey = CapabilityKey.defaults()
        .setKeyId(mKeyId)
        .setEncodedKey("guessedKey".getBytes())
        .setExpirationTimeMs(CommonUtils.getCurrentMs() + 100 * 1000);
    Capability capability = CapabilityUtils.generateCapability(wrongKey, mReadContent);
    try {
      CapabilityUtils.verifyAuthenticator(mKey, capability);
      Assert.fail("Verify authenticator generated with a wrong secret key should fail.");
    } catch (InvalidCapabilityException e) {
      // expected.
    }
  }

  @Test
  public void verifyAuthenticatorWithNewerAndCurKeysTest() throws Exception {
    Capability capability = CapabilityUtils.generateCapability(mKey, mReadContent);
    CapabilityKey newerKey = CapabilityKey.defaults()
        .setKeyId(mKeyId + 1)
        .setEncodedKey(mEncodingKey.getBytes())
        .setExpirationTimeMs(CommonUtils.getCurrentMs() + 100 * 1000);
    Assert.assertTrue(CapabilityUtils.verifyAuthenticator(newerKey, mKey, capability));
  }

  @Test
  public void verifyAuthenticatorWithOlderAndCurKeysTest() throws Exception {
    Capability capability = CapabilityUtils.generateCapability(mKey, mWriteContent);
    CapabilityKey olderKey = CapabilityKey.defaults()
        .setKeyId(mKeyId - 1)
        .setEncodedKey(mEncodingKey.getBytes())
        .setExpirationTimeMs(CommonUtils.getCurrentMs() + 100 * 1000);
    Assert.assertTrue(CapabilityUtils.verifyAuthenticator(mKey, olderKey, capability));
  }

  @Test
  public void verifyCapabilityKeyTestWithExpiredCapability() throws Exception {
    CapabilityContent expiredContent = CapabilityContent.defaults()
        .setOwner(mUsername)
        .setKeyId(mKeyId)
        .setFileId(mFileId)
        .setAccessMode(CapabilityContent.AccessMode.READ)
        .setExpirationTimeMs(CommonUtils.getCurrentMs() - 10 * 1000);
    Capability expiredCapability = CapabilityUtils.generateCapability(mKey, expiredContent);
    try {
      CapabilityUtils.verifyAuthenticator(mKey, expiredCapability);
      Assert.fail("Verify authenticator with an expired capability should fail.");
    } catch (InvalidCapabilityException e) {
      // expected.
    }
  }

  @Test
  public void verifyCapabilityContentTest() throws Exception {
    Assert.assertTrue(CapabilityUtils.verifyContent(mReadContent, mUsername, mFileId,
        CapabilityContent.AccessMode.READ));
    Assert.assertTrue(CapabilityUtils.verifyContent(mWriteContent, mUsername, mFileId,
        CapabilityContent.AccessMode.WRITE));
  }

  @Test
  public void verifyCapabilityContentWithDifferentUserTest() throws Exception {
    try {
      CapabilityUtils.verifyContent(mReadContent, "alice", mFileId,
          CapabilityContent.AccessMode.READ);
      Assert.fail("Verify content with a mismatching user should fail.");
    } catch (InvalidCapabilityException e) {
      // expected
    }
  }

  @Test
  public void verifyCapabilityContentWithDifferentFileTest() throws Exception {
    try {
      CapabilityUtils.verifyContent(mReadContent, mUsername, mFileId + 1,
          CapabilityContent.AccessMode.READ);
      Assert.fail("Verify content with a mismatching fild id should fail.");
    } catch (InvalidCapabilityException e) {
      // expected
    }
  }

  @Test
  public void verifyCapabilityContentWithDifferentModeTest() throws Exception {
    try {
      CapabilityUtils.verifyContent(mReadContent, mUsername, mFileId,
          CapabilityContent.AccessMode.WRITE);
      Assert.fail("Verify content with an unqualified access mode should fail.");
    } catch (InvalidCapabilityException e) {
      // expected
    }

    try {
      CapabilityUtils.verifyContent(mWriteContent, mUsername, mFileId,
          CapabilityContent.AccessMode.READ);
      Assert.fail("Verify content with an unqualified access mode should fail.");
    } catch (InvalidCapabilityException e) {
      // expected
    }
  }
}
