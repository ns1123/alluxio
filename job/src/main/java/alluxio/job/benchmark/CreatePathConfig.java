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

package alluxio.job.benchmark;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class CreatePathConfig extends AbstractThroughputLatencyJobConfig {
  private static final long serialVersionUID = 7859013978084941882L;

  public static final String NAME = "FSMasterCreateFile";

  private int mLevel;
  private int mDirSize;
  private boolean mIsDirectory;
  private boolean mUseFileSystemClient;

  /**
   * Creates an instance of AbstractThroughputAndLatencyJobConfig.
   *
   * @param level the number of lvels of the file system tree
   * @param dirSize the number of files or directories each non-leaf directory has
   * @param isDirectory whether the leaves are directories or files
   * @param useFileSystemClient whether to use {@link alluxio.client.file.FileSystem} in
   *                            the benchmark
   * @param expectedThroughput the expected throughput
   * @param threadNum the number of client threads
   * @param cleanUp whether to clean up after the test
   */
  public CreatePathConfig(@JsonProperty("level") int level,
      @JsonProperty("dirSize") int dirSize, @JsonProperty("isDirectory") boolean isDirectory,
      @JsonProperty("useFS") boolean useFileSystemClient,
      @JsonProperty("throughput") double expectedThroughput,
      @JsonProperty("threadNum") int threadNum, @JsonProperty("cleanUp") boolean cleanUp) {
    super((int) Math.pow(dirSize, level), expectedThroughput, threadNum, FileSystemType.ALLUXIO,
        true, cleanUp);
    mDirSize = dirSize;
    mLevel = level;
    mIsDirectory = isDirectory;
    mUseFileSystemClient = useFileSystemClient;
  }

  /**
   * @return the level size
   */
  public int getLevel() {
    return mLevel;
  }

  /**
   * @return the directory size
   */
  public int getDirSize() {
    return mDirSize;
  }

  /**
   * @return true if the leaves are directories
   */
  public boolean isDirectory() {
    return mIsDirectory;
  }

  /**
   * @return true if the benchmark uses {@link alluxio.client.file.FileSystem} directly.
   */
  public boolean isUseFileSystemClient() {
    return mUseFileSystemClient;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(" level: ");
    sb.append(mLevel);
    sb.append(" dirSize: ");
    sb.append(mDirSize);
    sb.append(" isDirectory: ");
    sb.append(mIsDirectory);
    return sb.toString();
  }
}
