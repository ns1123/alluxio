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
import alluxio.exception.ExceptionMessage;
import alluxio.retry.CountingRetry;
import alluxio.security.group.GroupMappingService;

import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

/**
 * A {@link GroupMappingService} for fetching user group mappings from an LDAP server.
 */
public final class LdapGroupsMapping implements GroupMappingService {
  private static final Logger LOG = LoggerFactory.getLogger(LdapGroupsMapping.class);
  private static final int LDAP_SERVER_REQUEST_RETRY_COUNT = 3;
  private static final String SIMPLE_AUTHENTICATION = "simple";
  private static final String SSL = "ssl";
  private static final String SSL_KEYSTORE_KEY = "javax.net.ssl.keyStore";
  private static final String SSL_KEYSTORE_PASSWORD_KEY = "javax.net.ssl.keyStorePassword";

  private DirContext mDirContext;

  /**
   * Constructs a new {@link LdapGroupsMapping}.
   * If the internal {@link DirContext} fails to be initialized, runtime error happens.
   */
  public LdapGroupsMapping() {}

  @Override
  public List<String> getGroups(String user) throws IOException {
    CountingRetry retry = new CountingRetry(LDAP_SERVER_REQUEST_RETRY_COUNT);
    while (retry.attempt()) {
      // Initialize the context if necessary.
      if (mDirContext == null) {
        try {
          mDirContext = createDirContext();
        } catch (IOException | NamingException e) {
          throw new IOException(ExceptionMessage.CANNOT_INITIALIZE_DIR_CONTEXT.getMessage(e));
        }
      }
      // Search for groups.
      try {
        return searchForGroups(user);
      } catch (NamingException e) {
        LOG.error(ExceptionMessage.CANNOT_GET_GROUPS_FROM_LDAP_SERVER.getMessage(user,
            retry.getAttemptCount()), e);
      }
      // Reset the context.
      mDirContext = null;
    }
    throw new IOException(ExceptionMessage.CANNOT_GET_GROUPS_FROM_LDAP_SERVER.getMessage(user,
        retry.getAttemptCount()));
  }

  /**
   * Searches for groups the user belongs to from the LDAP server.
   *
   * @param user the user
   * @return the user's groups
   * @throws NamingException when the search request fails
   */
  private List<String> searchForGroups(String user) throws NamingException {
    List<String> groups = new ArrayList<>();
    // Configure SearchControls.
    SearchControls searchControls = new SearchControls();
    searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchControls.setTimeLimit(
        Configuration.getInt(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SEARCH_TIMEOUT));
    // Limit the attributes returned to only those required to speed up the search.
    searchControls.setReturningAttributes(new String[]{
        Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_ATTR_GROUP_NAME)
    });
    // Get user's distinguished name from the LDAP server.
    String searchBase = Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_BASE);
    LOG.debug("LDAP search base = " + searchBase);
    String userQuery =
        Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_USER);
    LOG.debug("user query = " + userQuery);
    NamingEnumeration<SearchResult> results = mDirContext.search(searchBase, userQuery,
        new Object[]{user}, searchControls);
    if (results.hasMoreElements()) {
      String userDistinguishedName = results.nextElement().getNameInNamespace();
      LOG.debug("user DN = " + userDistinguishedName);
      String groupQuery = String.format("(&%s(%s={0}))",
          Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_GROUP),
          Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_ATTR_MEMBER));
      LOG.debug("group query = " + groupQuery);
      // Search for the user's groups.
      NamingEnumeration<SearchResult> groupResults =
          mDirContext.search(searchBase, groupQuery, new Object[]{userDistinguishedName},
              searchControls);
      while (groupResults.hasMoreElements()) {
        SearchResult groupResult = groupResults.nextElement();
        Attribute groupName = groupResult.getAttributes().get(
            Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_ATTR_GROUP_NAME));
        groups.add(groupName.get().toString());
      }
    }
    LOG.debug("groups = " + groups);
    return groups;
  }

  /**
   * @return a new {@link DirContext} based on the configuration
   * @throws IOException when password cannot be read from the password file
   * @throws NamingException when the {@link DirContext} fails to be created
   */
  DirContext createDirContext() throws IOException, NamingException {
    // Set context environments.
    Hashtable<String, String> env = new Hashtable<>();
    // Use LDAP context.
    env.put(Context.INITIAL_CONTEXT_FACTORY, com.sun.jndi.ldap.LdapCtxFactory.class.getName());
    // Set LDAP server URL.
    env.put(Context.PROVIDER_URL, Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_URL));
    // Set SSL configurations.
    if (Configuration.isSet(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL)
        && Configuration.getBoolean(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL)) {
      env.put(Context.SECURITY_PROTOCOL, SSL);
      System.setProperty(SSL_KEYSTORE_KEY,
          Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE));
      System.setProperty(SSL_KEYSTORE_PASSWORD_KEY, getPassword(
          PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD,
          PropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE));
    }
    // Set LDAP authentication configurations.
    env.put(Context.SECURITY_AUTHENTICATION, SIMPLE_AUTHENTICATION);
    env.put(Context.SECURITY_PRINCIPAL,
        Configuration.get(PropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_USER));
    env.put(Context.SECURITY_CREDENTIALS, getPassword(
        PropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD,
        PropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD_FILE));
    return new InitialDirContext(env);
  }

  /**
   * Gets the password from the configuration.
   *
   * @param passwordKey key for the value of the password
   * @param passwordFileKey key for the value of the password file
   * @return the value of passwordKey if available or the key contained in the password file
   * @throws IOException when password cannot be read from the password file
   */
  String getPassword(PropertyKey passwordKey, PropertyKey passwordFileKey)
      throws IOException {
    // Get the password if it is directly configured.
    if (Configuration.isSet(passwordKey)) {
      String password = Configuration.get(passwordKey);
      return password;
    }

    String passwordFile = "";
    if (Configuration.isSet(passwordFileKey)) {
      // Read from the file containing the password.
      passwordFile = Configuration.get(passwordFileKey);
    }
    if (passwordFile.isEmpty()) {
      return "";
    }
    StringBuilder passwordBuilder = new StringBuilder();
    try (Reader reader = new InputStreamReader(new FileInputStream(passwordFile), Charsets.UTF_8)) {
      int c = reader.read();
      while (c > -1) {
        passwordBuilder.append((char) c);
        c = reader.read();
      }
      return passwordBuilder.toString().trim();
    } catch (IOException e) {
      throw new IOException(ExceptionMessage.CANNOT_READ_PASSWORD_FILE.getMessage(passwordFile), e);
    }
  }
}
