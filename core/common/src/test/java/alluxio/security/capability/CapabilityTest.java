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

import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link Capability}.
 */
public final class CapabilityTest {
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

  @Test
  public void capabilityCreateTest() throws Exception {
    Capability capability = new Capability(mKey, mReadContent);
    Assert.assertEquals(mReadContent, capability.getCapabilityContent());
    Assert.assertFalse(capability.getAuthenticator().isEmpty());
  }

  @Test
  public void emptyCapabilityKey() throws Exception {
    CapabilityKey emptyEncodedKey = CapabilityKey.defaults()
        .setKeyId(mKeyId)
        .setEncodedKey("".getBytes())
        .setExpirationTimeMs(CommonUtils.getCurrentMs() + 100 * 1000);
    try {
      Capability capability = new Capability(emptyEncodedKey, mReadContent);
      Assert.fail("Creating capability with an empty encoded key should fail.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
