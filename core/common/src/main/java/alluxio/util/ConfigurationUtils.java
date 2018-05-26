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

package alluxio.util;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.annotation.Nullable;

/**
 * Utilities for working with Alluxio configurations.
 */
public final class ConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

  private ConfigurationUtils() {} // prevent instantiation
  // ALLUXIO CS ADD

  /**
   * Gets the RPC addresses of all masters based on the configuration.
   *
   * @return the master rpc addresses
   */
  public static java.util.List<java.net.InetSocketAddress> getMasterRpcAddresses() {
    if (Configuration.containsKey(PropertyKey.MASTER_RPC_ADDRESSES)) {
      return parseInetSocketAddresses(Configuration.getList(PropertyKey.MASTER_RPC_ADDRESSES, ","));
    } else {
      int rpcPort = alluxio.util.network.NetworkAddressUtils
          .getPort(alluxio.util.network.NetworkAddressUtils.ServiceType.MASTER_RPC);
      return getRpcAddresses(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, rpcPort);
    }
  }

  /**
   * Gets the RPC addresses of all job masters based on the configuration.
   *
   * @return the job master rpc addresses
   */
  public static java.util.List<java.net.InetSocketAddress> getJobMasterRpcAddresses() {
    int jobRpcPort = alluxio.util.network.NetworkAddressUtils
        .getPort(alluxio.util.network.NetworkAddressUtils.ServiceType.JOB_MASTER_RPC);
    if (Configuration.containsKey(PropertyKey.JOB_MASTER_RPC_ADDRESSES)) {
      return parseInetSocketAddresses(
          Configuration.getList(PropertyKey.JOB_MASTER_RPC_ADDRESSES, ","));
    } else if (Configuration.containsKey(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES)) {
      return getRpcAddresses(PropertyKey.JOB_MASTER_EMBEDDED_JOURNAL_ADDRESSES, jobRpcPort);
    } else if (Configuration.containsKey(PropertyKey.MASTER_RPC_ADDRESSES)) {
      return getRpcAddresses(PropertyKey.MASTER_RPC_ADDRESSES, jobRpcPort);
    } else {
      return getRpcAddresses(PropertyKey.MASTER_EMBEDDED_JOURNAL_ADDRESSES, jobRpcPort);
    }
  }

  /**
   * @param addressesKey configuration key for a list of addresses
   * @param overridePort the port to use
   * @return a list of inet addresses using the hostnames from addressesKey with the port
   *         overridePort
   */
  private static java.util.List<java.net.InetSocketAddress> getRpcAddresses(
      PropertyKey addressesKey, int overridePort) {
    java.util.List<java.net.InetSocketAddress> addresses =
        parseInetSocketAddresses(Configuration.getList(addressesKey, ","));
    java.util.List<java.net.InetSocketAddress> newAddresses =
        new java.util.ArrayList<>(addresses.size());
    for (java.net.InetSocketAddress addr : addresses) {
      newAddresses.add(new java.net.InetSocketAddress(addr.getHostName(), overridePort));
    }
    return newAddresses;
  }

  /**
   * @param addresses a list of address strings in the form "hostname:port"
   * @return a list of InetSocketAddresses representing the given address strings
   */
  private static java.util.List<java.net.InetSocketAddress> parseInetSocketAddresses(
      java.util.List<String> addresses) {
    java.util.List<java.net.InetSocketAddress> inetSocketAddresses =
        new java.util.ArrayList<>(addresses.size());
    for (String address : addresses) {
      try {
        inetSocketAddresses
            .add(alluxio.util.network.NetworkAddressUtils.parseInetSocketAddress(address));
      } catch (IOException e) {
        throw new IllegalArgumentException("Failed to parse host:port: " + address, e);
      }
    }
    return inetSocketAddresses;
  }
  // ALLUXIO CS END

  /**
   * Loads properties from resource. This method will search Classpath for the properties file with
   * the given resourceName.
   *
   * @param resourceName filename of the properties file
   * @return a set of properties on success, or null if failed
   */
  public static Properties loadPropertiesFromResource(String resourceName) {
    Properties properties = new Properties();

    InputStream inputStream =
        Configuration.class.getClassLoader().getResourceAsStream(resourceName);
    if (inputStream == null) {
      return null;
    }

    try {
      properties.load(inputStream);
    } catch (IOException e) {
      LOG.warn("Unable to load default Alluxio properties file {} : {}", resourceName,
          e.getMessage());
      return null;
    }
    return properties;
  }

  /**
   * Loads properties from the given file. This method will search Classpath for the properties
   * file.
   *
   * @param filePath the absolute path of the file to load properties
   * @return a set of properties on success, or null if failed
   */
  @Nullable
  public static Properties loadPropertiesFromFile(String filePath) {
    Properties properties = new Properties();

    try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
      properties.load(fileInputStream);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      LOG.warn("Unable to load properties file {} : {}", filePath, e.getMessage());
      return null;
    }
    return properties;
  }

  /**
   * Searches the given properties file from a list of paths as well as the classpath.
   *
   * @param propertiesFile the file to load properties
   * @param confPathList a list of paths to search the propertiesFile
   * @return the site properties file on success search, or null if failed
   */
  @Nullable
  public static String searchPropertiesFile(String propertiesFile,
      String[] confPathList) {
    if (propertiesFile == null || confPathList == null) {
      return null;
    }
    for (String path : confPathList) {
      String file = PathUtils.concatPath(path, propertiesFile);
      Properties properties = loadPropertiesFromFile(file);
      if (properties != null) {
        // If a site conf is successfully loaded, stop trying different paths.
        return file;
      }
    }
    return null;
  }
  // ALLUXIO CS ADD
  /**
   * @return whether the configuration describes how to find the job master host, either through
   *         explicit configuration or through zookeeper
   */
  public static boolean jobMasterHostConfigured() {
    boolean usingZk = Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)
        && Configuration.containsKey(PropertyKey.ZOOKEEPER_ADDRESS);
    return Configuration.containsKey(PropertyKey.JOB_MASTER_HOSTNAME) || usingZk
        || getJobMasterRpcAddresses().size() > 1;
  }
  // ALLUXIO CS END

  /**
   * @return whether the configuration describes how to find the master host, either through
   *         explicit configuration or through zookeeper
   */
  public static boolean masterHostConfigured() {
    boolean usingZk = Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)
        && Configuration.containsKey(PropertyKey.ZOOKEEPER_ADDRESS);
    // ALLUXIO CS REPLACE
    // return Configuration.containsKey(PropertyKey.MASTER_HOSTNAME) || usingZk;
    // ALLUXIO CS WITH
    return Configuration.containsKey(PropertyKey.MASTER_HOSTNAME) || usingZk
        || getMasterRpcAddresses().size() > 1;
    // ALLUXIO CS END
  }

  /**
   * @param value the value or null (value is not set)
   * @return the value or "(no value set)" when the value is not set
   */
  public static String valueAsString(String value) {
    return value == null ? "(no value set)" : value;
  }
}
