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

import alluxio.Configuration;
import alluxio.underfs.UnderFileSystem;

import org.junit.Test;

public class JDBCUnderFileSystemFactoryTest {

  /**
   * This test ensures the local UFS module correctly accepts paths that begin with / or file://.
   */
  @Test
  public void factoryTest() {
    UnderFileSystem ufs = new JDBCUnderFileSystemFactory().create(
        "jdbc:mysql://ec2-54-209-22-16.compute-1.amazonaws"
            + ".com:3306/testdb?table=testTable&user=test&password=test",
            new Configuration(), null);
  }
}
