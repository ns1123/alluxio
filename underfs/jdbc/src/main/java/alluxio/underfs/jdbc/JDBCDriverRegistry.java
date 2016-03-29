/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.jdbc;

import java.util.HashSet;
import java.util.Set;

/**
 * TODO(gpang) implement.
 */
public final class JDBCDriverRegistry {
  private static final Set<String> DRIVERS = new HashSet<>();

  private JDBCDriverRegistry() {} // Prevent instantiation

  /**
   * Loads a JDBC driver class, if it has not been previously loaded.
   *
   * @param className the name of the driver class to load
   */
  public static void loadDriver(String className) {
    if (DRIVERS.contains(className)) {
      return;
    }
    if (className != null) {
      try {
        Class.forName(className);
        DRIVERS.add(className);
      } catch (ClassNotFoundException e) {
        // Ignore this error.
      }
    }
  }
}
