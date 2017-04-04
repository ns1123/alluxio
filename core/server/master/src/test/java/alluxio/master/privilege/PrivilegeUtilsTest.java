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

package alluxio.master.privilege;

import static org.junit.Assert.assertEquals;

import alluxio.wire.Privilege;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link PrivilegeUtils}.
 */
public final class PrivilegeUtilsTest {
  @Test
  public void convertPrivileges() {
    for (Privilege p : Privilege.values()) {
      assertEquals(p, PrivilegeUtils.fromProto(PrivilegeUtils.toProto(p)));
    }
  }

  @Test
  public void convertPrivilegeList() {
    List<Privilege> ps = Arrays.asList(Privilege.PIN, Privilege.REPLICATION);
    assertEquals(ps, PrivilegeUtils.fromProto(PrivilegeUtils.toProto(ps)));
  }

  @Test
  public void convertEmptyPrivilegeList() {
    List<Privilege> ps = new ArrayList<>();
    assertEquals(ps, PrivilegeUtils.fromProto(PrivilegeUtils.toProto(ps)));
  }
}
