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

package alluxio.security.group.provider;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

import javax.naming.CommunicationException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

/**
 * Unit test for {@link alluxio.security.group.provider.LdapGroupsMapping}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(LdapGroupsMapping.class)
public final class LdapGroupsMappingTest {
  private static final String USER = "user";
  private static final String NON_EXISTENT_USER = "non-existent-user";
  private static final String USER_DISTINGUISHED_NAME = "CN=" + USER + ",DC=test,DC=com";
  private static final String[] GROUPS = new String[]{"group1", "group2"};
  private static final String GROUP_ATTR_NAME =
      PropertyKey.SECURITY_GROUP_MAPPING_LDAP_ATTR_GROUP_NAME.getDefaultValue();

  private DirContext mMockDirContext;
  private LdapGroupsMapping mMockMapping;
  private NamingEnumeration mMockUserNamingEnum;
  private NamingEnumeration mMockGroupNamingEnum;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    // TODO(cc): This should be called automatically by the junit framework before the @Before
    // method, but it's not called, investigate why.
    mTestFolder.create();

    mMockDirContext = Mockito.mock(DirContext.class);
    mMockMapping = PowerMockito.spy(new LdapGroupsMapping());
    mMockUserNamingEnum = Mockito.mock(NamingEnumeration.class);
    mMockGroupNamingEnum = Mockito.mock(NamingEnumeration.class);

    // Use the mock DirContext in the mock LdapGroupsMapping.
    Mockito.doReturn(mMockDirContext).when(mMockMapping).createDirContext();

    // Mock user search result.
    SearchResult mockUserResult = Mockito.mock(SearchResult.class);
    Mockito.when(mockUserResult.getNameInNamespace()).thenReturn(USER_DISTINGUISHED_NAME);
    Mockito.when(mMockUserNamingEnum.hasMoreElements()).thenReturn(true);
    Mockito.when(mMockUserNamingEnum.nextElement()).thenReturn(mockUserResult);

    // Mock group search result.
    SearchResult[] mockGroupResults = new SearchResult[GROUPS.length];
    for (int i = 0; i < GROUPS.length; i++) {
      SearchResult result = Mockito.mock(SearchResult.class);
      Attribute attr = new BasicAttribute(GROUP_ATTR_NAME);
      attr.add(GROUPS[i]);
      Attributes attrs = new BasicAttributes();
      attrs.put(attr);
      Mockito.when(result.getAttributes()).thenReturn(attrs);
      mockGroupResults[i] = result;
    }
    Mockito.when(mMockGroupNamingEnum.hasMoreElements()).thenReturn(true, true, false);
    Mockito.when(mMockGroupNamingEnum.nextElement()).thenReturn(
        mockGroupResults[0], mockGroupResults[1]);
  }

  @Test
  public void getGroups() throws Exception {
    Mockito.when(mMockDirContext.search(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(Object[].class), Mockito.any(SearchControls.class)))
        .thenReturn(mMockUserNamingEnum, mMockGroupNamingEnum);
    // Calls search for 2 times, one for getting the user's distinguished name, another for getting
    // the user's groups.
    testGetGroups(Arrays.asList(GROUPS), 2);
  }

  @Test
  public void getGroupsWhenNetworkError() throws Exception {
    // Simulates an network connection error by throwing a CommunicationException.
    Mockito.when(mMockDirContext.search(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(Object[].class), Mockito.any(SearchControls.class)))
        .thenThrow(new CommunicationException("Connection is closed"))
        .thenReturn(mMockUserNamingEnum, mMockGroupNamingEnum);

    // Calls search for 3 times, the first call for the user's distinguished name fails due to
    // network connection error, then in the second retry, one call for the user and another for
    // the groups.
    testGetGroups(Arrays.asList(GROUPS), 3);
  }

  @Test
  public void getGroupsWhenLdapServerDown() throws Exception {
    // Simulates the case where the LDAP server is down by always throwing CommunicationException.
    Mockito.when(mMockDirContext.search(Mockito.anyString(), Mockito.anyString(),
        Mockito.any(Object[].class), Mockito.any(SearchControls.class)))
        .thenThrow(new CommunicationException("Connection is closed"));
    mThrown.expect(IOException.class);
    mMockMapping.getGroups(USER);
  }

  @Test
  public void getGroupsForNonExistentUser() throws Exception {
    NamingEnumeration noResults = Mockito.mock(NamingEnumeration.class);
    Mockito.when(noResults.hasMoreElements()).thenReturn(false);
    Mockito.when(mMockDirContext.search(Mockito.anyString(), Mockito.anyString(),
        Mockito.eq(new Object[]{NON_EXISTENT_USER}), Mockito.any(SearchControls.class)))
        .thenReturn(noResults);
    Assert.assertEquals(Arrays.asList(new String[]{}), mMockMapping.getGroups(NON_EXISTENT_USER));
  }

  private void testGetGroups(List<String> expectedGroups, int searchTimes) throws Exception {
    List<String> groups = mMockMapping.getGroups(USER);
    Assert.assertEquals(expectedGroups, groups);
    Mockito.verify(mMockDirContext, Mockito.times(searchTimes)).search(Mockito.anyString(),
        Mockito.anyString(), Mockito.any(Object[].class), Mockito.any(SearchControls.class));
  }

  @Test
  public void getPassword() throws Exception {
    final String expectedPassword = "password";

    // Tests getting password from the property value directly.
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD, expectedPassword);
    LdapGroupsMapping mapping = new LdapGroupsMapping();
    String password = mapping.getPassword(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD,
        PropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD_FILE);
    Assert.assertEquals(expectedPassword, password);

    // Tests the case where no password or password file is set.
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD, "");
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE, "");
    password = mapping.getPassword(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD,
        PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE);
    Assert.assertEquals("", password);

    // Setup a password file.
    File passwordFile = mTestFolder.newFile();
    Writer writer = new FileWriter(passwordFile);
    writer.write(expectedPassword);
    writer.close();

    // Tests getting password from the password file.
    Configuration.set(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE,
        passwordFile.getAbsolutePath());
    password = mapping.getPassword(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD,
        PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE);
    Assert.assertEquals(expectedPassword, password);
  }
}
