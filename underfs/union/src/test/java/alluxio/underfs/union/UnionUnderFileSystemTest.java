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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.ConfigurationTestUtils;
import alluxio.collections.IndexedSet;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.mock.MockUnderFileSystem;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for the {@link UnionUnderFileSystem}.
 */
public class UnionUnderFileSystemTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Tests that options are propagated to nested UFSes.
   */
  @Test
  public void defaultOptionsPass() {
    Map<String, String> properties = defaultOptions();
    UnderFileSystem ufs =
        UnderFileSystem.Factory.create("union://test", UnderFileSystemConfiguration
            .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));

    IndexedSet<UfsKey> rp = retrieveInternal(ufs, "mUnderFileSystem>mUfses");
    Set<UfsKey> cp = retrieveInternal(ufs, "mUnderFileSystem>mCreateUfses");

    // See if internal sets are the proper size
    assertEquals(3, rp.size());
    assertEquals(1, cp.size());

    // Make sure priorities are correct
    assertEquals(0, rp.getFirstByField(UnionUnderFileSystem.ALIAS_IDX, "B").getPriority());
    assertEquals(1, rp.getFirstByField(UnionUnderFileSystem.ALIAS_IDX, "A").getPriority());
    assertEquals(2, rp.getFirstByField(UnionUnderFileSystem.ALIAS_IDX, "C").getPriority());
    assertEquals(-1, cp.iterator().next().getPriority()); // all creates should be -1

    // Make sure the nested UFS has the options
    PropertyKey pkFoo = PropertyKey.fromString("foo");
    PropertyKey pkBar = PropertyKey.fromString("bar");
    String getProps = "mDelegate>mUnderFileSystem>mUfsConf>mProperties";
    IndexedSet<UfsKey> idxCp = new IndexedSet<>(UnionUnderFileSystem.ALIAS_IDX,
        UnionUnderFileSystem.PRIORITY_IDX);
    idxCp.addAll(cp);
    AlluxioProperties props = retrieveInternal(
        idxCp.getFirstByField(UnionUnderFileSystem.ALIAS_IDX, "A").getUfs(), getProps);
    assertEquals(props.get(pkFoo), "1");

    props = retrieveInternal(
        rp.getFirstByField(UnionUnderFileSystem.ALIAS_IDX, "A").getUfs(), getProps);
    assertEquals(props.get(pkFoo), "1");

    props = retrieveInternal(
        rp.getFirstByField(UnionUnderFileSystem.ALIAS_IDX, "B").getUfs(), getProps);
    assertEquals(props.get(pkBar), "2");
  }

  @Test
  public void failNoReadPriority() {
    mThrown.expect(IllegalArgumentException.class);
    Map<String, String> properties = defaultOptions();
    properties.remove("alluxio-union.priority.read");
    UnderFileSystem.Factory.create("union://test", UnderFileSystemConfiguration
            .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));
  }

  @Test
  public void failDuplicateReadPriority() {
    mThrown.expect(IllegalArgumentException.class);
    Map<String, String> properties = defaultOptions();
    properties.put("alluxio-union.priority.read", "A,A,B");
    UnderFileSystem.Factory.create("union://test", UnderFileSystemConfiguration
        .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));
  }

  @Test
  public void failNotEnoughReadPriority() {
    mThrown.expect(IllegalArgumentException.class);
    Map<String, String> properties = defaultOptions();
    properties.put("alluxio-union.priority.read", "A,B");
    UnderFileSystem.Factory.create("union://test", UnderFileSystemConfiguration
        .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));
  }

  @Test
  public void failNoCreatePriority() {
    mThrown.expect(IllegalArgumentException.class);
    Map<String, String> properties = defaultOptions();
    properties.remove("alluxio-union.collection.create");
    UnderFileSystem.Factory.create("union://test", UnderFileSystemConfiguration
        .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));
  }

  @Test
  public void failDuplicateCreatePriorty() {
    mThrown.expect(IllegalArgumentException.class);
    Map<String, String> properties = defaultOptions();
    properties.put("alluxio-union.collection.create", "A,A");
    UnderFileSystem.Factory.create("union://test", UnderFileSystemConfiguration
        .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));
  }

  @Test
  public void failEmptyCreatePriority() {
    mThrown.expect(IllegalArgumentException.class);
    Map<String, String> properties = defaultOptions();
    properties.put("alluxio-union.collection.create", "");
    UnderFileSystem.Factory.create("union://test", UnderFileSystemConfiguration
        .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));
  }

  /**
   * Test the operation mode method under different maintenance scenarios.
   */
  @Test
  public void getOperationMode() {
    Map<String, String> properties = new HashMap<>();
    String uriA = "mock://A/";
    String uriB = "mock://B/";
    properties.put("alluxio-union.B.uri", uriB);
    properties.put("alluxio-union.A.uri", uriA);
    properties.put("alluxio-union.priority.read", "A,B");
    properties.put("alluxio-union.collection.create", "A");
    UnderFileSystem ufs =
        UnderFileSystem.Factory.create("union://test", UnderFileSystemConfiguration
            .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));
    Map<String, UfsMode> physicalUfsState = new HashMap<>();
    // Check default
    assertEquals(UfsMode.READ_WRITE, ufs.getOperationMode(physicalUfsState));
    // Check single ufs down
    physicalUfsState.put(uriA, UfsMode.NO_ACCESS);
    assertEquals(UfsMode.READ_ONLY, ufs.getOperationMode(physicalUfsState));
    // Check single ufs read only
    physicalUfsState.put(uriA, UfsMode.READ_ONLY);
    assertEquals(UfsMode.READ_ONLY, ufs.getOperationMode(physicalUfsState));
    // Check both ufs down
    physicalUfsState.put(uriA, UfsMode.NO_ACCESS);
    physicalUfsState.put(uriB, UfsMode.NO_ACCESS);
    assertEquals(UfsMode.NO_ACCESS, ufs.getOperationMode(physicalUfsState));
  }

  @Test
  public void testAliasParsing() throws Exception {
    UnionUnderFileSystem ufs = createFromDefaultOptions();

    // No authority = default return
    Collection<UfsKey> c1 = ufs.getUfsInputs("union:///path/to/file", Collections.emptyList());
    assertEquals(Collections.emptyList(), c1);

    // No scheme = default return
    c1 = ufs.getUfsInputs("/path/to/file", Collections.emptyList());
    assertEquals(Collections.emptyList(), c1);

    // Test each alias
    c1 = ufs.getUfsInputs("union://A/path/to/file", Collections.emptyList());
    assertEquals(1, c1.size());
    c1.forEach((ufsKey) -> assertEquals("A", ufsKey.getAlias()));

    c1 = ufs.getUfsInputs("union://B/path/to/file", Collections.emptyList());
    assertEquals(1, c1.size());
    c1.forEach((ufsKey) -> assertEquals("B", ufsKey.getAlias()));

    c1 = ufs.getUfsInputs("union://C/path/to/file", Collections.emptyList());
    assertEquals(1, c1.size());
    c1.forEach((ufsKey) -> assertEquals("C", ufsKey.getAlias()));
  }

  @Test
  public void ufsInputNonUnionScheme() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage(
        String.format("must only be %s in Union UFS", UnionUnderFileSystem.SCHEME));
    UnionUnderFileSystem ufs = createFromDefaultOptions();
    ufs.getUfsInputs("hdfs://host:20202/path/to/file", Collections.emptyList());
  }

  @Test
  public void ufsInputNonExistingAlias() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("No alias");
    UnionUnderFileSystem ufs = createFromDefaultOptions();
    Collection<UfsKey> a = ufs.getUfsInputs("union://BadAlias/path/to/file",
        Collections.emptyList());
    System.out.println(a);
  }

  @Test
  public void testBasicCreateApis() throws Exception {
    createTest(UnderFileSystem::createNonexistingFile, mock(OutputStream.class));
    createTest(UnderFileSystem::create, mock(OutputStream.class));
    createTest(UnderFileSystem::mkdirs, Boolean.TRUE);
  }

  @Test
  public void testBasicSplitInvokeApis() throws Exception {
    UfsDirectoryStatus dirStatusMock = mock(UfsDirectoryStatus.class);
    when(dirStatusMock.getXAttr()).thenReturn(null);
    when(dirStatusMock.withXAttr(anyMapOf(String.class, byte[].class))).thenReturn(dirStatusMock);
    UfsFileStatus fileStatusMock = mock(UfsFileStatus.class);
    when(fileStatusMock.getXAttr()).thenReturn(null);
    when(fileStatusMock.withXAttr(anyMapOf(String.class, byte[].class))).thenReturn(fileStatusMock);
    UfsStatus ufsStatusMock = mock(UfsFileStatus.class);
    when(ufsStatusMock.getXAttr()).thenReturn(null);
    when(ufsStatusMock.withXAttr(anyMapOf(String.class, byte[].class))).thenReturn(ufsStatusMock);

    testSplitInvokeApis(UnderFileSystem::getAclPair, mock(alluxio.collections.Pair.class));
    testSplitInvokeApis(UnderFileSystem::getBlockSizeByte, 10L);
    testSplitInvokeApis(UnderFileSystem::getDirectoryStatus, dirStatusMock);
    testSplitInvokeApis(UnderFileSystem::getExistingDirectoryStatus, dirStatusMock);
    testSplitInvokeApis(UnderFileSystem::getFileStatus, ufsStatusMock);
    testSplitInvokeApis(UnderFileSystem::getExistingFileStatus, ufsStatusMock);
    testSplitInvokeApis(UnderFileSystem::getFileLocations, mock(List.class));
    // Doesn't work because interface doesn't throw IOException
    // testSplitInvokeApis(UnderFileSystem::getFingerprint, "testFingerprint");
    testSplitInvokeApis(UnderFileSystem::getStatus, ufsStatusMock);
    testSplitInvokeApis(UnderFileSystem::getExistingStatus, ufsStatusMock);
    testSplitInvokeApis(UnderFileSystem::isDirectory, false);
    testSplitInvokeApis(UnderFileSystem::isExistingDirectory, false);
    testSplitInvokeApis(UnderFileSystem::isFile, false);

    UfsStatus[] statuses = { mock(UfsFileStatus.class) };
    when(statuses[0].getName()).thenReturn("testName");
    testSplitInvokeApis(UnderFileSystem::listStatus, statuses);
  }

  private <T> void createTest(UncheckedBiFunction<UnderFileSystem, String, T> ufsFunc,
      T returnObj) throws Exception {
    // Test the common case
    Map<String, String> props = defaultOptions();
    props.put("alluxio-union.collection.create", "A,C");
    Pair<UnionUnderFileSystem, Map<String, MockUnderFileSystem>> info =
        createUnionUfsFromOptions(props);
    UnionUnderFileSystem ufs = info.getLeft();
    Map<String, MockUnderFileSystem> mocks = info.getRight();

    when(ufsFunc.apply(mocks.get("A"), "mock://A/file")).thenReturn(returnObj)
        .thenReturn(returnObj);
    when(ufsFunc.apply(mocks.get("C"), "mock://C/file"))
        .thenReturn(returnObj);

    ufsFunc.apply(ufs, "union:///file");

    ufsFunc.apply(verify(mocks.get("A")), "mock://A/file");
    ufsFunc.apply(verify(mocks.get("C")), "mock://C/file");
    ufsFunc.apply(verify(mocks.get("B"), never()), any());

    ufsFunc.apply(ufs, "/file");

    ufsFunc.apply(verify(mocks.get("A"), times(2)), "mock://A/file");
    ufsFunc.apply(verify(mocks.get("C"), times(2)), "mock://C/file");
    ufsFunc.apply(verify(mocks.get("B"), never()), any());

    // Test when an exception is thrown in one method
    props.put("alluxio-union.collection.create", "A,C");
    info = createUnionUfsFromOptions(props);
    ufs = info.getLeft();
    mocks = info.getRight();

    when(ufsFunc.apply(mocks.get("A"), "mock://A/file"))
        .thenThrow(new IOException());
    when(ufsFunc.apply(mocks.get("C"), "mock://C/file"))
        .thenReturn(returnObj);

    try {
      ufsFunc.apply(ufs, "union:///file");
      fail("Should not have succeeded creating output stream with underlying failure.");
    } catch (IOException e) {
      // Expect this exception
    }

    ufsFunc.apply(verify(mocks.get("A")), "mock://A/file");
    ufsFunc.apply(verify(mocks.get("C")), "mock://C/file");
    ufsFunc.apply(verify(mocks.get("B"), never()), any());

    // Test when a specific UFS is desired
    props.put("alluxio-union.collection.create", "A,C");
    info = createUnionUfsFromOptions(props);
    ufs = info.getLeft();
    mocks = info.getRight();

    when(ufsFunc.apply(mocks.get("B"), "mock://B/file")).thenReturn(returnObj);

    ufsFunc.apply(ufs, "union://B/file");

    ufsFunc.apply(verify(mocks.get("B")), "mock://B/file");
    ufsFunc.apply(verify(mocks.get("A"), never()), any());
    ufsFunc.apply(verify(mocks.get("C"), never()), any());

    // Test when an invalid alias is passed
    try {
      ufsFunc.apply(ufs, "union://D/file");
      fail("Exception should be thrown on invalid UFS union alias");
    } catch (IOException e) {
      // Expect this exception
    }
  }

  /**
   * Tests UFS API calls that utilize split invoke with no hints.
   *
   * @param ufsFunc the ufs function to call. Parameters are the UFS and the input path
   * @param returnObj
   * @param <T>
   * @throws IOException
   */
  private <T> void testSplitInvokeApis(UncheckedBiFunction<UnderFileSystem, String, T> ufsFunc,
      T returnObj) throws IOException {

    // Common case, all UFSes get called, verify on each
    Map<String, String> props = defaultOptions();
    Pair<UnionUnderFileSystem, Map<String, MockUnderFileSystem>> info =
        createUnionUfsFromOptions(props);
    UnionUnderFileSystem ufs = info.getLeft();
    Map<String, MockUnderFileSystem> mocks = info.getRight();

    when(ufsFunc.apply(mocks.get("A"), "mock://A/file")).thenReturn(returnObj);
    when(ufsFunc.apply(mocks.get("B"), "mock://B/file")).thenReturn(returnObj);
    when(ufsFunc.apply(mocks.get("C"), "mock://C/file")).thenReturn(returnObj);

    ufsFunc.apply(ufs, "union:///file");

    ufsFunc.apply(verify(mocks.get("A")), "mock://A/file");
    ufsFunc.apply(verify(mocks.get("B")), "mock://B/file");
    ufsFunc.apply(verify(mocks.get("C")), "mock://C/file");

    ufsFunc.apply(ufs, "/file");

    ufsFunc.apply(verify(mocks.get("A"), times(2)), "mock://A/file");
    ufsFunc.apply(verify(mocks.get("B"), times(2)), "mock://B/file");
    ufsFunc.apply(verify(mocks.get("C"), times(2)), "mock://C/file");

    // Test the case where a single UFS is specified
    info = createUnionUfsFromOptions(props);
    ufs = info.getLeft();
    mocks = info.getRight();

    when(ufsFunc.apply(mocks.get("A"), "mock://A/file")).thenReturn(returnObj);

    ufsFunc.apply(ufs, "union://A/file");

    ufsFunc.apply(verify(mocks.get("A")), "mock://A/file");
    ufsFunc.apply(verify(mocks.get("B"), never()), any());
    ufsFunc.apply(verify(mocks.get("C"), never()), any());

    // Test the case where one UFS throws an error
    // We expect this to still succeed, as its possible for one UFS to not contain a file
    info = createUnionUfsFromOptions(props);
    ufs = info.getLeft();
    mocks = info.getRight();

    when(ufsFunc.apply(mocks.get("A"), "mock://A/file")).thenThrow(new IOException());
    when(ufsFunc.apply(mocks.get("B"), "mock://B/file")).thenReturn(returnObj);
    when(ufsFunc.apply(mocks.get("C"), "mock://C/file")).thenReturn(returnObj);

    try {
      ufsFunc.apply(ufs, "/file");
    } catch (IOException e) {
      fail("Exception should not have been thrown with only one splitInvoke failure");
    }

    ufsFunc.apply(verify(mocks.get("A")), "mock://A/file");
    ufsFunc.apply(verify(mocks.get("B")), "mock://B/file");
    ufsFunc.apply(verify(mocks.get("C")), "mock://C/file");

    // Test the case where all UFS throw an error
    // We expect this to fail and treat it like a failure from any other UFS
    info = createUnionUfsFromOptions(props);
    ufs = info.getLeft();
    mocks = info.getRight();

    when(ufsFunc.apply(mocks.get("A"), "mock://A/file")).thenThrow(new IOException());
    when(ufsFunc.apply(mocks.get("B"), "mock://B/file")).thenThrow(new IOException());
    when(ufsFunc.apply(mocks.get("C"), "mock://C/file")).thenThrow(new IOException());

    try {
      ufsFunc.apply(ufs, "/file");
      fail("Should not have succeeded on splitInvoke when all calls throw exceptions");
    } catch (IOException e) {
      // Expected
    }

    ufsFunc.apply(verify(mocks.get("A")), "mock://A/file");
    ufsFunc.apply(verify(mocks.get("B")), "mock://B/file");
    ufsFunc.apply(verify(mocks.get("C")), "mock://C/file");

    // Test when an invalid alias is passed
    try {
      ufsFunc.apply(ufs, "union://D/file");
      fail("Exception should be thrown on invalid UFS union alias");
    } catch (IOException e) {
      // Expect this exception
    }
  }

  private Map<String, String> defaultOptions() {
    Map<String, String> properties = new HashMap<>();
    properties.put("alluxio-union.A.uri", "mock://A");
    properties.put("alluxio-union.B.uri", "mock://B");
    properties.put("alluxio-union.C.uri", "mock://C");
    properties.put("alluxio-union.A.option.foo", "1");
    properties.put("alluxio-union.B.option.bar", "2");
    properties.put("alluxio-union.priority.read", "B,A,C");
    properties.put("alluxio-union.collection.create", "A");
    return properties;
  }

  private UnionUnderFileSystem createFromDefaultOptions() {
    return createFromOptions(defaultOptions());
  }

  private UnionUnderFileSystem createFromOptions(Map<String, String> properties) {
    return new UnionUnderFileSystem(UnderFileSystemConfiguration
        .defaults(ConfigurationTestUtils.defaults()).createMountSpecificConf(properties));
  }

  /**
   * @return the union UFS and a map of UFS alias to mocked UFS
   */
  private Pair<UnionUnderFileSystem, Map<String, MockUnderFileSystem>> createUnionUfsFromOptions(
      Map<String, String> options) {
    UnionUnderFileSystem unionUfs = createFromOptions(options);

    IndexedSet<UfsKey> ufskeys = retrieveInternal(unionUfs, "mUfses");
    Map<String, MockUnderFileSystem> mocks = new HashMap<>();
    ufskeys.forEach(ufsKey -> {
      String alias = ufsKey.getAlias();
      // Get the underlying mockFS
      MockUnderFileSystem ufs = mock(MockUnderFileSystem.class);
      setInternal(ufsKey, "mUfs>mDelegate>mUnderFileSystem", ufs);
      mocks.put(alias, ufs);
    });
    return Pair.of(unionUfs, mocks);
  }

  private <T> T retrieveInternal(Object o, String fields) {
    List<String> items =
        Lists.newArrayList(Splitter.on(">").trimResults().omitEmptyStrings().split(fields));
    Object tmp = o;
    for (String field : items) {
      tmp = Whitebox.getInternalState(tmp, field);
    }
    return (T) tmp;
  }

  private <T> void setInternal(Object o, String fields, T item) {
    List<String> items =
        Lists.newArrayList(Splitter.on(">").trimResults().omitEmptyStrings().split(fields));
    Object tmp = o;
    for (int i = 0; i < items.size(); i++) {
      if (i < items.size() - 1) { // while not the last item
        tmp = Whitebox.getInternalState(tmp, items.get(i));
      } else {
        Whitebox.setInternalState(tmp, items.get(i), item);
      }
    }
  }
}
