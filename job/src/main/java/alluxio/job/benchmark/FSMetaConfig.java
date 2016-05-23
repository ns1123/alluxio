/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.client.WriteType;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * The configuration for FSMetaDefinition.
 */
public final class FSMetaConfig extends AbstractThroughputLatencyJobConfig {
  private static final long serialVersionUID = 7859013978084941882L;

  public static final String NAME = "FSMeta";

  private Command mCommand;
  private int mLevel;
  private int mLevelIgnored;
  private int mDirSize;
  private WriteType mWriteType;
  private boolean mUseFileSystemClient;

  /**
   * The command types supported by FSMeta benchmark.
   */
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
   * Creates an instance of FSMetaConfig.
   *
   * @param command the {@link Command} to execute
   * @param level the number of levels of the file system tree
   *              {@link FSMetaDefinition#getWorkDir(AbstractThroughputLatencyJobConfig, int)}
   *              is level 0.
   * @param levelIgnored the number of levels to ignore (counted from the bottom of the file
   *                     system tree)
   * @param dirSize the number of files or directories each non-leaf directory has
   * @param useFileSystemClient whether to use {@link alluxio.client.file.FileSystem} in
   *                            the benchmark
   * @param expectedThroughput the expected throughput
   * @param writeType the alluxio file write type
   * @param workDir the working directory
   * @param threadNum the number of client threads
   * @param cleanUp whether to clean up after the test
   */
  public FSMetaConfig(@JsonProperty("command") String command, @JsonProperty("level") int level,
      @JsonProperty("levelIgnored") int levelIgnored, @JsonProperty("dirSize") int dirSize,
      @JsonProperty("useFS") boolean useFileSystemClient,
      @JsonProperty("throughput") double expectedThroughput,
      @JsonProperty("writeType") String writeType,
      @JsonProperty("workDir") String workDir,
      @JsonProperty("threadNum") int threadNum, @JsonProperty("cleanUp") boolean cleanUp) {
    super((int) Math.round(Math.pow(dirSize, level)), expectedThroughput, workDir, threadNum,
        "ALLUXIO", true, cleanUp);
    mCommand = Command.valueOf(command);
    mDirSize = dirSize;
    mLevel = level;
    mLevelIgnored = levelIgnored;
    mUseFileSystemClient = useFileSystemClient;
    mWriteType = WriteType.valueOf(writeType);
    Preconditions.checkState(mLevelIgnored < mLevel);
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
   * @return the number of levels to ignore counting from the bottom of the file system tree
   */
  public int getLevelIgnored() {
    return mLevelIgnored;
  }

  /**
   * @return the directory size
   */
  public int getDirSize() {
    return mDirSize;
  }

  /**
   * @return the write type
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @return true if the benchmark uses {@link alluxio.client.file.FileSystem} directly
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
    sb.append(" levelIgnored: ");
    sb.append(mLevelIgnored);
    sb.append(" useFS: ");
    sb.append(mUseFileSystemClient);
    sb.append(" dirSize: ");
    sb.append(mDirSize);
    return sb.toString();
  }
}
