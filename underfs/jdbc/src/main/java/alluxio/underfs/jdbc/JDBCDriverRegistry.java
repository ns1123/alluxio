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

package alluxio.underfs.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class takes care of JDBC driver class loading.
 */
@NotThreadSafe
public final class JDBCDriverRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCDriverRegistry.class);
  private static final Set<String> DRIVERS = new HashSet<>();

  private JDBCDriverRegistry() {} // Prevent instantiation

  /**
   * Loads a JDBC driver class, if it has not been previously loaded.
   *
   * @param className the name of the driver class to load
   */
  public static void load(String className) {
    if (DRIVERS.contains(className)) {
      return;
    }
    if (className != null) {
      try {
        Class cls = Class.forName(className);
        if (cls.newInstance() instanceof Driver) {
          DRIVERS.add(className);
        } else {
          LOG.error("Class {} is not an instance of java.sql.Driver", className);
        }
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        // Ignore this error.
        LOG.error("Failed to load class name: {}, error: {}", className, e.getMessage());
      }
    }
  }
}
