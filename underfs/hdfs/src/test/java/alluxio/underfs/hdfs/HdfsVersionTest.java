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

package alluxio.underfs.hdfs;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link HdfsVersion}.
 */
public class HdfsVersionTest {

  @Test
  public void find() throws Exception {
    Assert.assertNull(HdfsVersion.find("NotValid"));
    for (HdfsVersion version : HdfsVersion.values()) {
      Assert.assertEquals(version, HdfsVersion.find(version.getCanonicalVersion()));
    }
  }

  @Test
  public void findByHadoopLabel() throws Exception {
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("1.0.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("1.0.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("hadoop-1.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("hadoop-1.0.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_0, HdfsVersion.find("hadoop1.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("1.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("1.2.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("hadoop-1.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("hadoop-1.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_1_2, HdfsVersion.find("hadoop1.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("2.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("2.2.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("hadoop-2.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("hadoop-2.2.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_2, HdfsVersion.find("hadoop2.2"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("2.3.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("2.3.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("hadoop-2.3"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("hadoop-2.3.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_3, HdfsVersion.find("hadoop2.3"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("2.4.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("2.4.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("hadoop-2.4"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("hadoop-2.4.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_4, HdfsVersion.find("hadoop2.4"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("2.5.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("2.5.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("hadoop-2.5"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("hadoop-2.5.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_5, HdfsVersion.find("hadoop2.5"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("2.6.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("2.6.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("hadoop-2.6"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("hadoop-2.6.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_6, HdfsVersion.find("hadoop2.6"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("2.7.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("2.7.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("hadoop-2.7"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("hadoop-2.7.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_7, HdfsVersion.find("hadoop2.7"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("2.8.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("2.8.0-SNAPSHOT"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("hadoop-2.8"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("hadoop-2.8.0"));
    Assert.assertEquals(HdfsVersion.HADOOP_2_8, HdfsVersion.find("hadoop2.8"));
    Assert.assertEquals(HdfsVersion.CDH_5_6, HdfsVersion.find("2.6.0-cdh5.6.0"));
    Assert.assertEquals(HdfsVersion.CDH_5_6, HdfsVersion.find("cdh-5.6"));
    Assert.assertEquals(HdfsVersion.CDH_5_6, HdfsVersion.find("cdh5.6"));
    Assert.assertEquals(HdfsVersion.CDH_5_8, HdfsVersion.find("2.6.0-cdh5.8.0"));
    Assert.assertEquals(HdfsVersion.CDH_5_8, HdfsVersion.find("cdh-5.8"));
    Assert.assertEquals(HdfsVersion.CDH_5_8, HdfsVersion.find("cdh5.8"));
    Assert.assertEquals(HdfsVersion.CDH_5_11, HdfsVersion.find("2.6.0-cdh5.11.0"));
    Assert.assertEquals(HdfsVersion.CDH_5_11, HdfsVersion.find("cdh-5.11"));
    Assert.assertEquals(HdfsVersion.CDH_5_11, HdfsVersion.find("cdh5.11"));
    Assert.assertEquals(HdfsVersion.CDH_5_12, HdfsVersion.find("2.6.0-cdh5.12.1"));
    Assert.assertEquals(HdfsVersion.CDH_5_12, HdfsVersion.find("cdh-5.12"));
    Assert.assertEquals(HdfsVersion.CDH_5_12, HdfsVersion.find("cdh5.12"));
    Assert.assertEquals(HdfsVersion.CDH_5_13, HdfsVersion.find("2.6.0-cdh5.13.1"));
    Assert.assertEquals(HdfsVersion.CDH_5_13, HdfsVersion.find("cdh-5.13"));
    Assert.assertEquals(HdfsVersion.CDH_5_13, HdfsVersion.find("cdh5.13"));
    Assert.assertEquals(HdfsVersion.CDH_5_14, HdfsVersion.find("2.6.0-cdh5.14.0"));
    Assert.assertEquals(HdfsVersion.CDH_5_14, HdfsVersion.find("cdh-5.14"));
    Assert.assertEquals(HdfsVersion.CDH_5_14, HdfsVersion.find("cdh5.14"));
    Assert.assertEquals(HdfsVersion.HDP_2_4, HdfsVersion.find("2.7.1.2.4.0.0-169"));
    Assert.assertEquals(HdfsVersion.HDP_2_4, HdfsVersion.find("hdp-2.4"));
    Assert.assertEquals(HdfsVersion.HDP_2_4, HdfsVersion.find("hdp2.4"));
    Assert.assertEquals(HdfsVersion.HDP_2_5, HdfsVersion.find("2.7.3.2.5.0.0-1245"));
    Assert.assertEquals(HdfsVersion.HDP_2_5, HdfsVersion.find("hdp-2.5"));
    Assert.assertEquals(HdfsVersion.HDP_2_5, HdfsVersion.find("hdp2.5"));
    Assert.assertEquals(HdfsVersion.MAPR_5_2, HdfsVersion.find("maprfs-5.2"));
    Assert.assertEquals(HdfsVersion.MAPR_5_2, HdfsVersion.find("2.7.0-mapr-1607"));
    Assert.assertEquals(HdfsVersion.MAPR_4_1, HdfsVersion.find("2.5.1-mapr-1503"));
  }
}
