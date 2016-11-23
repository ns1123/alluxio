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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link Capability}.
 */
public final class CapabilityTest {
  private final long mKeyId = 1L;
  private final long mFileId = 2L;
  private final String mEncodingKey = "mykey";
  private final String mUsername = "testuser";

  private final CapabilityProto.Content mReadContent = CapabilityProto.Content.newBuilder()
      .setUser(mUsername)
      .setFileId(mFileId)
      .setAccessMode(Mode.Bits.READ.ordinal())
      .setExpirationTimeMs(CommonUtils.getCurrentMs() + 10 * 1000).build();

  private final CapabilityProto.Content mWriteContent = CapabilityProto.Content.newBuilder()
      .setUser(mUsername)
      .setFileId(mFileId)
      .setAccessMode(Mode.Bits.WRITE.ordinal())
      .setExpirationTimeMs(CommonUtils.getCurrentMs() + 10 * 1000).build();

  private CapabilityKey mKey;

  @Before
  public void before() throws Exception {
    mKey = new CapabilityKey(
        mKeyId, CommonUtils.getCurrentMs() + 100 * 1000, mEncodingKey.getBytes());
  }

  @Test
  public void capabilityCreate() throws Exception {
    Capability capability = new Capability(mKey, mReadContent);
    Assert.assertEquals(mReadContent, capability.getContentDecoded());
    Assert.assertEquals(mReadContent, CapabilityProto.Content.parseFrom(capability.getContent()));
    Assert.assertNotEquals(0, capability.getAuthenticator().length);
  }

  @Test
  public void capabilityFromThrift() throws Exception {
    alluxio.thrift.Capability capabilityThrift = new Capability(mKey, mReadContent).toThrift();
    Capability capability = new Capability(capabilityThrift);
    Assert.assertEquals(mReadContent, capability.getContentDecoded());
    Assert.assertEquals(mReadContent, CapabilityProto.Content.parseFrom(capability.getContent()));
    Assert.assertNotEquals(0, capability.getAuthenticator().length);
  }

  @Test
  public void invalidThriftCapability() throws Exception {
    alluxio.thrift.Capability capabilityThrift = new Capability(mKey, mReadContent).toThrift();
    capabilityThrift.setContent((byte[]) null);
    boolean invalidCapability = false;
    try {
      new Capability(capabilityThrift);
    } catch (InvalidCapabilityException e) {
      invalidCapability = true;
    }
    Assert.assertTrue(invalidCapability);
  }

  @Test
  public void verifyAuthenticator() throws Exception {
    alluxio.thrift.Capability capabilityThrift = new Capability(mKey, mReadContent).toThrift();
    capabilityThrift
        .setContent(mReadContent.toBuilder().setUser("wronguser").build().toByteArray());
    Capability capability = new Capability(capabilityThrift);
    try {
      capability.verifyAuthenticator(mKey);
      Assert.fail("Changed content should fail to authenticate.");
    } catch (InvalidCapabilityException e) {
      // expected
    }
  }

  @Test
  public void verifyAuthenticatorTestWithNewerKeyId() throws Exception {
    Capability capability = new Capability(mKey, mReadContent);
    CapabilityKey newerKey = new CapabilityKey(
        mKeyId + 1, CommonUtils.getCurrentMs() + 100 * 1000, mEncodingKey.getBytes());
    try {
      capability.verifyAuthenticator(newerKey);
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
    Capability capability = new Capability(mKey, mReadContent);
    try {
      capability.verifyAuthenticator(wrongKey);
      Assert.fail("Verify authenticator generated with a wrong secret key should fail.");
    } catch (InvalidCapabilityException e) {
      // expected.
    }
  }

  @Test
  public void verifyAuthenticatorWithNewerAndCurKeysTest() throws Exception {
    Capability capability = new Capability(mKey, mReadContent);
    CapabilityKey newerKey = new CapabilityKey(
        mKeyId + 1, CommonUtils.getCurrentMs() + 100 * 1000, "gussedKey".getBytes());
    capability.verifyAuthenticator(newerKey, mKey);
  }

  @Test
  public void verifyAuthenticatorWithOlderAndCurKeysTest() throws Exception {
    Capability capability = new Capability(mKey, mWriteContent);
    CapabilityKey olderKey = new CapabilityKey(
        mKeyId - 1, CommonUtils.getCurrentMs() + 100 * 1000, "guessedKey".getBytes());
    capability.verifyAuthenticator(mKey, olderKey);
  }
}
