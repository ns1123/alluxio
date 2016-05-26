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

package alluxio.underfs;

import javax.annotation.concurrent.ThreadSafe;

/**
 * UnderFileSystem constants.
 */
@ThreadSafe
public final class UnderFileSystemConstants {
  private UnderFileSystemConstants() {} // prevent instantiation

  // UnderFileSystem properties constants.

  // JDBC properties.
  public static final String JDBC_DRIVER_CLASS = "jdbc.driver.class";
  public static final String JDBC_PARTITIONS = "jdbc.partitions";
  public static final String JDBC_TABLE = "jdbc.table";
  public static final String JDBC_PARTITION_KEY = "jdbc.partition.key";
  public static final String JDBC_WHERE = "jdbc.where";
  public static final String JDBC_PROJECTION = "jdbc.projection";
  public static final String JDBC_USER = "jdbc.user";
  public static final String JDBC_PASSWORD = "jdbc.password";

  // JDBC properties. These are computed by the UFS for a particular table.
  // The prefix of the property name for the partition conditions. The partition index is appended.
  public static final String JDBC_PROJECTION_CONDITION_PREFIX = "jdbc.partition.condition";

  // JDBC properties. These are computed by the UFS for a particular partition.
  // The computed JDBC partition filename.
  public static final String JDBC_PARTITION_FILENAME = "jdbc.partition.filename";
}
