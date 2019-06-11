/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class UnionUnderFileSystemUtilsTest {

  @Test
  public void testMergeUfsStatus() {
    UfsKey a = new UfsKey("A", 0, Mockito.mock(UnderFileSystem.class));
    UfsKey b = new UfsKey("B", 1, Mockito.mock(UnderFileSystem.class));
    UfsStatus mocked = new UfsFileStatus("test", "test", 1, 2, "test", "test", (short) 3);
    SortedMap<UfsKey, UfsStatus> keyToStatus = new TreeMap<>();

    // Test the case with two UFSes, each returning a separate UFSstatus for the same path
    keyToStatus.put(a, mocked);
    keyToStatus.put(b, Mockito.mock(UfsStatus.class));
    UfsStatus result = UnionUnderFileSystemUtils.mergeUfsStatusXAttr(keyToStatus);
    assertEquals("test", result.getName());
    assertNotNull(result.getXAttr());
    assertEquals(2, result.getXAttr().size());

    // Test the case where there is only one UFS and one status
    keyToStatus.clear();
    keyToStatus.put(a, mocked);
    result = UnionUnderFileSystemUtils.mergeUfsStatusXAttr(keyToStatus);
    assertNotNull(result.getXAttr());
    assertEquals(1, result.getXAttr().size());

    // Test the case where there are no entries
    keyToStatus.clear();
    result = UnionUnderFileSystemUtils.mergeUfsStatusXAttr(keyToStatus);
    assertNull(result);
  }

  @Test
  public void testMergeListStatus() {
    UfsKey a = new UfsKey("A", 0, Mockito.mock(UnderFileSystem.class));
    UfsKey b = new UfsKey("B", 1, Mockito.mock(UnderFileSystem.class));
    UfsKey c = new UfsKey("C", 2, Mockito.mock(UnderFileSystem.class));
    UfsStatus mocked1 = new UfsFileStatus("test", "test", 1, 2, "test", "test", (short) 3);
    UfsStatus mocked2 = new UfsFileStatus("test2", "test2", 1, 2, "test2", "test2", (short) 3);
    UfsStatus mocked3 = new UfsFileStatus("test3", "test3", 1, 2, "test3", "test3", (short) 3);

    // Test the case where each UFS returns a different UFS status
    SortedMap<UfsKey, UfsStatus[]> map = new TreeMap<>();
    map.put(a, new UfsStatus[]{ mocked1 });
    map.put(b, new UfsStatus[]{ mocked2 });
    map.put(c, new UfsStatus[]{ mocked3 });
    UfsStatus[] result = UnionUnderFileSystemUtils.mergeListStatusXAttr(map);
    assertNotNull(result);
    assertEquals(3, result.length);
    List<UfsStatus> l = Arrays.asList(result);
    Map<String, UfsStatus> statuses = new HashMap<>();
    l.forEach((k) -> statuses.put(k.getName(), k));
    assertEquals(1, statuses.get("test").getXAttr().size());
    assertEquals(1, statuses.get("test2").getXAttr().size());
    assertEquals(1, statuses.get("test3").getXAttr().size());

    // Test the case where each UFS may return a status with the same name as another UFS
    map.clear();
    statuses.clear();
    map.put(a, new UfsStatus[]{ mocked1, mocked3 });
    map.put(b, new UfsStatus[]{ mocked2 });
    map.put(c, new UfsStatus[]{ mocked3, mocked1 });
    result = UnionUnderFileSystemUtils.mergeListStatusXAttr(map);
    assertNotNull(result);
    assertEquals(3, result.length);
    l = Arrays.asList(result);
    l.forEach((k) -> statuses.put(k.getName(), k));
    assertEquals(2, statuses.get("test").getXAttr().size());
    assertEquals(1, statuses.get("test2").getXAttr().size());
    assertEquals(2, statuses.get("test3").getXAttr().size());

    // Test when one UFS returns null statuses
    map.clear();
    statuses.clear();
    map.put(a, new UfsStatus[]{ mocked1, mocked2 });
    map.put(b, null);
    map.put(c, new UfsStatus[]{ mocked3 });
    result = UnionUnderFileSystemUtils.mergeListStatusXAttr(map);
    assertNotNull(result);
    assertEquals(3, result.length);
    l = Arrays.asList(result);
    l.forEach((k) -> statuses.put(k.getName(), k));
    assertEquals(1, statuses.get("test").getXAttr().size());
    assertEquals(1, statuses.get("test2").getXAttr().size());
    assertEquals(1, statuses.get("test3").getXAttr().size());

    // Test where all UFSes return a null status
    map.clear();
    statuses.clear();
    map.put(a, null);
    map.put(b, null);
    map.put(c, null);
    result = UnionUnderFileSystemUtils.mergeListStatusXAttr(map);
    assertNull(result);
  }
}
