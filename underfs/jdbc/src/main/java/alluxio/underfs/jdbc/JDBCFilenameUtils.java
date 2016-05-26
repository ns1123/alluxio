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

/**
 * Utility methods for dealing with filenames for JDBC UFS.
 */
public final class JDBCFilenameUtils {
  private static final String PREFIX = "part-";
  private static final int PREFIX_LENGTH = PREFIX.length();

  private JDBCFilenameUtils() {} // prevent instantiation

  /**
   * @param partition the partition index (0-indexed)
   * @param extension the file extension
   * @return the filename for a given partition index
   */
  public static String getFilenameForPartition(int partition, String extension) {
    if (extension == null || extension.isEmpty()) {
      return String.format("%s%06d", PREFIX, partition);
    }
    return String.format("%s%06d.%s", PREFIX, partition, extension);
  }

  /**
   * @param filename the name of the file to parse the partition index from
   * @return the partition index from the given filename, -1 if filename is invalid
   */
  public static int getPartitionFromFilename(String filename) {
    if (filename == null || !filename.startsWith(PREFIX)) {
      return -1;
    }
    int endIndex = filename.indexOf('.', PREFIX_LENGTH);
    if (endIndex == -1) {
      endIndex = filename.length();
    }
    String partStr = filename.substring(PREFIX_LENGTH, endIndex);
    return Integer.parseInt(partStr);
  }
}
