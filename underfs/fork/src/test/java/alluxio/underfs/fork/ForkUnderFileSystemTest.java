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

package alluxio.underfs.fork;

import alluxio.underfs.UfsMode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.mock.MockUnderFileSystem;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the {@link ForkUnderFileSystem}.
 */
public class ForkUnderFileSystemTest {

  /**
   * Tests that options are propagated to nested UFSes.
   */
  @Test
  public void options() {
    Map<String, String> properties = new HashMap<>();
    String uriA = "mock://A";
    String uriB = "mock://B";
    properties.put("alluxio-fork.A.ufs", uriA);
    properties.put("alluxio-fork.A.option.foo", "1");
    properties.put("alluxio-fork.B.ufs", uriB);
    properties.put("alluxio-fork.B.option.bar", "2");
    UnderFileSystem ufs = UnderFileSystem.Factory.create("alluxio-fork://test",
        UnderFileSystemConfiguration.defaults().setMountSpecificConf(properties));
    UnderFileSystem nestedUfs = Whitebox.getInternalState(ufs, "mUnderFileSystem");
    ImmutableMap<String, Pair<String, UnderFileSystem>> ufses =
        Whitebox.getInternalState(nestedUfs, "mUnderFileSystems");
    MockUnderFileSystem ufsA =
        Whitebox.getInternalState(ufses.get("A").getRight(), "mUnderFileSystem");
    MockUnderFileSystem ufsB =
        Whitebox.getInternalState(ufses.get("B").getRight(), "mUnderFileSystem");
    UnderFileSystemConfiguration propA = Whitebox.getInternalState(ufsA, "mUfsConf");
    UnderFileSystemConfiguration propB = Whitebox.getInternalState(ufsB, "mUfsConf");
    Assert.assertEquals(propA.getMountSpecificConf().size(), 1);
    Assert.assertEquals(propA.getMountSpecificConf().get("foo"), "1");
    Assert.assertEquals(propB.getMountSpecificConf().size(), 1);
    Assert.assertEquals(propB.getMountSpecificConf().get("bar"), "2");
  }

  /**
   * Tests that nested UFS are processed in the alphabetical order of their provider name.
   */
  @Test
  public void order() {
    Map<String, String> properties = new HashMap<>();
    String uriA = "mock://A";
    String uriB = "mock://B";
    String uriC = "mock://C";
    properties.put("alluxio-fork.B.ufs", uriB);
    properties.put("alluxio-fork.B.option.property", "Y");
    properties.put("alluxio-fork.A.ufs", uriA);
    properties.put("alluxio-fork.A.option.property", "X");
    properties.put("alluxio-fork.C.ufs", uriC);
    properties.put("alluxio-fork.C.option.property", "Z");
    UnderFileSystem ufs = UnderFileSystem.Factory.create("alluxio-fork://test",
        UnderFileSystemConfiguration.defaults().setMountSpecificConf(properties));
    UnderFileSystem nestedUfs = Whitebox.getInternalState(ufs, "mUnderFileSystem");
    ImmutableMap<String, Pair<String, UnderFileSystem>> ufses =
        Whitebox.getInternalState(nestedUfs, "mUnderFileSystems");
    List<String> values = new ArrayList<>();
    for (Pair<String, UnderFileSystem> entry : ufses.values()) {
      MockUnderFileSystem mockUfs = Whitebox.getInternalState(entry.getValue(), "mUnderFileSystem");
      values.add(mockUfs.getProperty("property"));
    }
    Assert.assertEquals(values.size(), 3);
    Assert.assertEquals(values.get(0), "X");
    Assert.assertEquals(values.get(1), "Y");
    Assert.assertEquals(values.get(2), "Z");
  }

  /**
   * Test the operation mode method under different maintenance scenarios.
   */
  @Test
  public void getOperationMode() {
    Map<String, String> properties = new HashMap<>();
    String uriA = "mock://A/";
    String uriB = "mock://B/";
    properties.put("alluxio-fork.B.ufs", uriB);
    properties.put("alluxio-fork.A.ufs", uriA);
    UnderFileSystem ufs = UnderFileSystem.Factory.create("alluxio-fork://test",
        UnderFileSystemConfiguration.defaults().setMountSpecificConf(properties));
    Map<String, UfsMode> physicalUfsState = new Hashtable<>();
    // Check default
    Assert.assertEquals(UfsMode.READ_WRITE, ufs.getOperationMode(physicalUfsState));
    // Check single ufs down
    physicalUfsState.put(uriA, UfsMode.NO_ACCESS);
    Assert.assertEquals(UfsMode.READ_ONLY, ufs.getOperationMode(physicalUfsState));
    // Check single ufs read only
    physicalUfsState.put(uriA, UfsMode.READ_ONLY);
    Assert.assertEquals(UfsMode.READ_ONLY, ufs.getOperationMode(physicalUfsState));
    // Check both ufs down
    physicalUfsState.put(uriA, UfsMode.NO_ACCESS);
    physicalUfsState.put(uriB, UfsMode.NO_ACCESS);
    Assert.assertEquals(UfsMode.NO_ACCESS, ufs.getOperationMode(physicalUfsState));
  }
}
