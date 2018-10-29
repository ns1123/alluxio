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

package alluxio.security.authentication;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DelegationTokenIdentifier}.
 */
public final class DelegationTokenIdentifierTest {

  @Test
  public void createDelegationTokenIdentifierFromThrift() throws Exception {
    DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 1L, 2L, 3L);
    Assert.assertEquals(id, DelegationTokenIdentifier.fromThrift(id.toThrift()));
  }

  @Test
  public void createDelegationTokenIdentifierFromProto() throws Exception {
    DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 1L, 2L, 3L);
    Assert.assertEquals(id, DelegationTokenIdentifier.fromProto(id.toProto()));
  }

  @Test
  public void createDelegationTokenIdentifierFromByteArray() throws Exception {
    DelegationTokenIdentifier id = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 1L, 2L, 3L);
    Assert.assertEquals(id, DelegationTokenIdentifier.fromByteArray(id.getBytes()));
  }

  @Test
  public void compareIdenticalDelegationTokens() throws Exception {
    DelegationTokenIdentifier id1 = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 1L, 2L, 3L);
    DelegationTokenIdentifier id2 = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 1L, 2L, 3L);
    Assert.assertEquals(id1, id2);
  }

  @Test
  public void compareDifferentDelegationTokens() throws Exception {
    DelegationTokenIdentifier id1 = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 1L, 2L, 3L);
    DelegationTokenIdentifier id2 = new DelegationTokenIdentifier("user", "renewer", "proxy",
        1L, 1L, 2L, 3L);
    DelegationTokenIdentifier id3 = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 2L, 2L, 3L);
    DelegationTokenIdentifier id4 = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 1L, 3L, 3L);
    DelegationTokenIdentifier id5 = new DelegationTokenIdentifier("user", "renewer", "proxy",
        0L, 1L, 2L, 4L);
    DelegationTokenIdentifier id6 = new DelegationTokenIdentifier("user2", "renewer", "proxy",
        0L, 1L, 2L, 3L);
    DelegationTokenIdentifier id7 = new DelegationTokenIdentifier("user", "renewer2", "proxy",
        0L, 1L, 2L, 3L);
    DelegationTokenIdentifier id8 = new DelegationTokenIdentifier("user", "renewer", "proxy2",
        0L, 1L, 2L, 3L);
    Assert.assertNotEquals(id1, id2);
    Assert.assertNotEquals(id1, id3);
    Assert.assertNotEquals(id1, id4);
    Assert.assertNotEquals(id1, id5);
    Assert.assertNotEquals(id1, id6);
    Assert.assertNotEquals(id1, id7);
    Assert.assertNotEquals(id1, id8);
    Assert.assertNotEquals(id1, null);
  }
}
