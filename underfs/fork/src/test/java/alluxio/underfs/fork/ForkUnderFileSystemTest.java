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

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.mock.MockUnderFileSystem;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
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
    UnderFileSystem ufs = UnderFileSystem.Factory.create("alluxio-fork://", properties);
    UnderFileSystem nestedUfs = Whitebox.getInternalState(ufs, "mUnderFileSystem");
    ImmutableMap<String, UnderFileSystem> ufses =
        Whitebox.getInternalState(nestedUfs, "mUnderFileSystems");
    MockUnderFileSystem ufsA = Whitebox.getInternalState(ufses.get(uriA), "mUnderFileSystem");
    MockUnderFileSystem ufsB = Whitebox.getInternalState(ufses.get(uriB), "mUnderFileSystem");
    Map<String, String> propA = ufsA.getProperties();
    Map<String, String> propB = ufsB.getProperties();
    Assert.assertEquals(propA.size(), 1);
    Assert.assertEquals(propA.get("foo"), "1");
    Assert.assertEquals(propB.size(), 1);
    Assert.assertEquals(propB.get("bar"), "2");
  }
}
