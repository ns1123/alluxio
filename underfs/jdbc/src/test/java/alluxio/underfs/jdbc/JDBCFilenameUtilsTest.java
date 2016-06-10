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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the methods of {@link JDBCFilenameUtils}.
 */
public class JDBCFilenameUtilsTest {
  @Test
  public void getFilenameForPartitionTest() {
    Assert.assertEquals("part-000000.csv", JDBCFilenameUtils.getFilenameForPartition(0, "csv"));
    Assert.assertEquals("part-000001.txt", JDBCFilenameUtils.getFilenameForPartition(1, "txt"));

    Assert.assertEquals("part-001234", JDBCFilenameUtils.getFilenameForPartition(1234, ""));
    Assert.assertEquals("part-001234", JDBCFilenameUtils.getFilenameForPartition(1234, null));
  }

  @Test
  public void getPartitionFromFilenameTest() {
    Assert.assertEquals(0, JDBCFilenameUtils.getPartitionFromFilename("part-000000.csv"));
    Assert.assertEquals(1, JDBCFilenameUtils.getPartitionFromFilename("part-000001.csv"));
    Assert.assertEquals(1234, JDBCFilenameUtils.getPartitionFromFilename("part-001234.txt"));
    Assert.assertEquals(1234, JDBCFilenameUtils.getPartitionFromFilename("part-001234"));

    Assert.assertEquals(-1, JDBCFilenameUtils.getPartitionFromFilename(null));
    Assert.assertEquals(-1, JDBCFilenameUtils.getPartitionFromFilename(""));
  }
}
