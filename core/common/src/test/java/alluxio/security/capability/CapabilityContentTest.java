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

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Unit tests for {@link CapabilityContent}.
 */
public final class CapabilityContentTest {
  @Test
  public void defaults() {
    CapabilityContent key = CapabilityContent.defaults();
    Assert.assertEquals(0L, key.getKeyId());
    Assert.assertEquals(0L, key.getExpirationTimeMs());
    Assert.assertNull(key.getOwner());
    Assert.assertEquals(0L, key.getFileId());
    Assert.assertNull(key.getAccessMode());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long keyid = random.nextLong();
    long expiration = random.nextLong();
    long fileid = random.nextLong();
    String owner = "testuser";
    CapabilityContent.AccessMode mode = CapabilityContent.AccessMode.READ;

    CapabilityContent key = CapabilityContent.defaults();
    key.setKeyId(keyid);
    key.setExpirationTimeMs(expiration);
    key.setOwner(owner);
    key.setFileId(fileid);
    key.setAccessMode(mode);

    Assert.assertEquals(keyid, key.getKeyId());
    Assert.assertEquals(expiration, key.getExpirationTimeMs());
    Assert.assertEquals(owner, key.getOwner());
    Assert.assertEquals(fileid, key.getFileId());
    Assert.assertEquals(mode, key.getAccessMode());
  }

  @Test
  public void accessMode() {
    CapabilityContent.AccessMode read = CapabilityContent.AccessMode.READ;
    CapabilityContent.AccessMode write = CapabilityContent.AccessMode.WRITE;
    CapabilityContent.AccessMode readWrite = CapabilityContent.AccessMode.READ_WRITE;

    Assert.assertTrue(read.hasRead());
    Assert.assertFalse(read.hasWrite());
    Assert.assertEquals("READ", read.toString());

    Assert.assertFalse(write.hasRead());
    Assert.assertTrue(write.hasWrite());
    Assert.assertEquals("WRITE", write.toString());

    Assert.assertTrue(readWrite.hasRead());
    Assert.assertTrue(readWrite.hasWrite());
    Assert.assertEquals("READ_WRITE", readWrite.toString());
  }

  @Test
  public void accessModeIncludes() {
    CapabilityContent.AccessMode read = CapabilityContent.AccessMode.READ;
    CapabilityContent.AccessMode write = CapabilityContent.AccessMode.WRITE;
    CapabilityContent.AccessMode readWrite = CapabilityContent.AccessMode.READ_WRITE;

    Assert.assertTrue(read.includes(read));
    Assert.assertFalse(read.includes(write));
    Assert.assertFalse(read.includes(readWrite));

    Assert.assertFalse(write.includes(read));
    Assert.assertTrue(write.includes(write));
    Assert.assertFalse(write.includes(readWrite));

    Assert.assertTrue(readWrite.includes(read));
    Assert.assertTrue(readWrite.includes(write));
    Assert.assertTrue(readWrite.includes(readWrite));
  }
}
