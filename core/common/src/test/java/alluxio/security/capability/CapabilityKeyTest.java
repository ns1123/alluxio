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

import javax.crypto.SecretKey;

/**
 * Tests for {@link CapabilityKey}.
 */
public final class CapabilityKeyTest {
  @Test
  public void defaults() {
    CapabilityKey key = CapabilityKey.defaults();
    Assert.assertEquals(0L, key.getKeyId());
    Assert.assertEquals(0L, key.getExpirationTimeMs());
    Assert.assertNull(key.getEncodedKey());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long keyid = random.nextLong();
    long expiration = random.nextLong();
    byte[] encodedKey = "test_key".getBytes();

    CapabilityKey key = CapabilityKey.defaults();
    key.setKeyId(keyid);
    key.setExpirationTimeMs(expiration);
    key.setEncodedKey(encodedKey);

    Assert.assertEquals(keyid, key.getKeyId());
    Assert.assertEquals(expiration, key.getExpirationTimeMs());
    Assert.assertEquals(new String(encodedKey), new String(key.getEncodedKey()));
  }

  @Test
  public void setNullKey() {
    CapabilityKey key = CapabilityKey.defaults();
    key.setEncodedKey((byte[]) null);
    Assert.assertNull(key.getEncodedKey());
    key.setEncodedKey((SecretKey) null);
    Assert.assertNull(key.getEncodedKey());
  }

  // TODO(chaomin): add tests for setting CapabilityKey with SecretKey.
}
