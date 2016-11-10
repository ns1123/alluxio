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

/**
 * Unit tests for {@link SecretManager}.
 */
public final class SecretManagerTest {
  @Test
  public void toHexStringTest() {
    Assert.assertNull(SecretManager.toHexString(null));
    Assert.assertEquals("30", SecretManager.toHexString("0".getBytes()));
    Assert.assertEquals("616263", SecretManager.toHexString("abc".getBytes()));
  }

  @Test
  public void calculateHMACTest() throws Exception {
    String expectedHMAC = "5d667a809eb0216e280889096a46d92f2c36db5f";
    Assert.assertEquals(expectedHMAC, SecretManager.calculateHMAC("mykey", "payload"));

    String expectedHMACForEmptyData = "5bb9c066a336f0e6f17d7ddac4e43de7a94a6c9a";
    Assert.assertEquals(expectedHMACForEmptyData, SecretManager.calculateHMAC("mykey", ""));

    try {
      SecretManager.calculateHMAC("", "payload");
      Assert.fail("Should get IllegalArgumentException with an empty key.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
