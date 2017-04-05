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

package alluxio.client.privilege.options;

import static org.junit.Assert.assertNotNull;

import alluxio.CommonTestUtils;

import org.junit.Test;

/**
 * Unit tests for {@link GetGroupPrivilegesOptions}.
 */
public final class GetGroupPrivilegesOptionsTest {
  /**
   * Tests that building a {@link GetGroupPrivilegesOptions} with the defaults works.
   */
  @Test
  public void defaults() {
    assertNotNull(GetGroupPrivilegesOptions.defaults());
  }

  /**
   * Tests conversion to thrift representation.
   */
  @Test
  public void toThrift() {
    assertNotNull(GetGroupPrivilegesOptions.defaults().toThrift());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(GetGroupPrivilegesOptions.class);
  }
}
