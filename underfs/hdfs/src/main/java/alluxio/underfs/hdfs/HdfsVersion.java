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

import java.util.regex.Pattern;

import javax.annotation.Nullable;

/** The set of supported Hdfs versions. */
public enum HdfsVersion {
  HADOOP_1_0("hadoop-1.0", "(hadoop-?1\\.0(\\.(\\d+))?|1\\.0(\\.(\\d+)(-.*)?)?)"),
  HADOOP_1_2("hadoop-1.2", "(hadoop-?1\\.2(\\.(\\d+))?|1\\.2(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_2("hadoop-2.2", "(hadoop-?2\\.2(\\.(\\d+))?|2\\.2(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_3("hadoop-2.3", "(hadoop-?2\\.3(\\.(\\d+))?|2\\.3(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_4("hadoop-2.4", "(hadoop-?2\\.4(\\.(\\d+))?|2\\.4(\\.(\\d+)(-.*)?)?)"),
  HADOOP_2_5("hadoop-2.5", "(hadoop-?2\\.5(\\.(\\d+))?|2\\.5(\\.(\\d+)(-(?!mapr).*)?)?)"),
  HADOOP_2_6("hadoop-2.6", "(hadoop-?2\\.6(\\.(\\d+))?|2\\.6(\\.(\\d+)(-(?!cdh).*)?)?)"),
  HADOOP_2_7("hadoop-2.7", "(hadoop-?2\\.7(\\.(\\d+))?|2\\.7(\\.(\\d+)(-(?!cdh|mapr).*)?)?)"),
  HADOOP_2_8("hadoop-2.8", "(hadoop-?2\\.8(\\.(\\d+))?|2\\.8(\\.(\\d+)(-(?!cdh).*)?)?)"),
  CDH_5_6("cdh-5.6", "(cdh-?5\\.6(\\.(\\d+))?|2\\.6\\.0-cdh5\\.6\\.(.*)?)"),
  CDH_5_8("cdh-5.8", "(cdh-?5\\.8(\\.(\\d+))?|2\\.6\\.0-cdh5\\.8\\.(.*)?)"),
  CDH_5_11("cdh-5.11", "(cdh-?5\\.11(\\.(\\d+))?|2\\.6\\.0-cdh5\\.11\\.(.*)?)"),
  CDH_5_12("cdh-5.12", "(cdh-?5\\.12(\\.(\\d+))?|2\\.6\\.0-cdh5\\.12\\.(.*)?)"),
  HDP_2_4("hdp-2.4", "(hdp-?2\\.4(\\.(\\d+))?|2\\.7\\.1\\.2\\.4\\.(\\d+)\\.(\\d+)-(.*)?)"),
  HDP_2_5("hdp-2.5", "(hdp-?2\\.5(\\.(\\d+))?|2\\.7\\.3\\.2\\.5\\.(\\d+)\\.(\\d+)-(.*)?)"),
  HDP_2_6("hdp-2.6", "(hdp-?2\\.6(\\.(\\d+))?|2\\.7\\.3\\.2\\.6\\.(\\d+)\\.(\\d+)-(.*)?)"),
  MAPR_4_1("maprfs-4.1", "(maprfs-?4\\.1|2\\.5\\.1-mapr-1503)"),
  MAPR_5_0("maprfs-5.0", "(maprfs-?5\\.0|2\\.7\\.0-mapr-1506)"),
  MAPR_5_1("maprfs-5.1", "(maprfs-?5\\.1|2\\.7\\.0-mapr-1602)"),
  MAPR_5_2("maprfs-5.2", "(maprfs-?5\\.2|2\\.7\\.0-mapr-1607)"),
  ;

  private final String mCanonicalVersion;
  private final Pattern mVersionPattern;

  /**
   * Constructs an instance of {@link HdfsVersion}.
   *
   * @param canonicalVersion the canonical version of an HDFS
   * @param versionPattern the regex pattern of version for an HDFS
   */
  HdfsVersion(String canonicalVersion, String versionPattern) {
    mCanonicalVersion = canonicalVersion;
    mVersionPattern = Pattern.compile(versionPattern);
  }

  /**
   * @param versionString given version string
   * @return the corresponding {@link HdfsVersion} instance
   */
  @Nullable
  public static HdfsVersion find(String versionString) {
    for (HdfsVersion version : HdfsVersion.values()) {
      if (version.mVersionPattern.matcher(versionString).matches()) {
        return version;
      }
    }
    return null;
  }

  /**
   * @return the canonical version string
   */
  public String getCanonicalVersion() {
    return mCanonicalVersion;
  }
}
