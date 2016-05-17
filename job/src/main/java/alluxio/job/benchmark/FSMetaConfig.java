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

import alluxio.thrift.Command;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class FSMetaConfig extends AbstractThroughputLatencyJobConfig {
  private static final long serialVersionUID = 7859013978084941882L;

  public static final String NAME = "FSMeta";

  private Command mCommand;
  private int mLevel;
  private int mDirSize;
  private boolean mUseFileSystemClient;

  public static enum Command {
    /** Creates file. */
    CREATE_FILE(0),

    /** Creates directory. */
    CREATE_DIR(1),

    /** Deletes a file or directory. */
    DELETE(2),

    /** Get status of a file or directory. */
    GET_STATUS(3),

    /** List status of a file or directory. */
    LIST_STATUS(4);

    private int mValue;

    Command(int value) {
      mValue = value;
    }

    int getValue() {
      return mValue;
    }
  }

  /**
   * Creates an instance of AbstractThroughputAndLatencyJobConfig.
   *
   * @param command the {@link Command} to execute
   * @param level the number of levels of the file system tree
   * @param dirSize the number of files or directories each non-leaf directory has
   * @param useFileSystemClient whether to use {@link alluxio.client.file.FileSystem} in
   *                            the benchmark
   * @param expectedThroughput the expected throughput
   * @param threadNum the number of client threads
   * @param cleanUp whether to clean up after the test
   */
  public FSMetaConfig(@JsonProperty("command") String command, @JsonProperty("level") int level,
      @JsonProperty("dirSize") int dirSize,
      @JsonProperty("useFS") boolean useFileSystemClient,
      @JsonProperty("throughput") double expectedThroughput,
      @JsonProperty("threadNum") int threadNum, @JsonProperty("cleanUp") boolean cleanUp) {
    super((int) Math.pow(dirSize, level), expectedThroughput, threadNum, FileSystemType.ALLUXIO,
        true, cleanUp);
    mCommand = Command.valueOf(command);
    mDirSize = dirSize;
    mLevel = level;
    mUseFileSystemClient = useFileSystemClient;
  }

  /**
   * @return the command to run
   */
  public Command getCommand() {
    return mCommand;
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
    sb.append(" commandType: ");
    sb.append(mCommand);
    sb.append(" level: ");
    sb.append(mLevel);
    sb.append(" dirSize: ");
    sb.append(mDirSize);
    sb.append(" isDirectory: ");
    return sb.toString();
  }
}
