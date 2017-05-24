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

package alluxio;

/**
 * Project constants from compilation time by maven.
 */
public final class ProjectConstants {
  // ALLUXIO CS ADD
  /* URL to the proxy server for license check and call home. **/
  public static final String PROXY_URL = "${proxy.url}";
  // ALLUXIO CS END
  /* Project version, specified in maven property. **/
  public static final String VERSION = "${project.version}";
  /* Hadoop version, specified in maven property. **/
  public static final String HADOOP_VERSION = "${hadoop.version}";

  private ProjectConstants() {} // prevent instantiation
}
