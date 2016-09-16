/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

/**
 * The configuration for FSMetaDefinition.
 */
@JsonTypeName(FSMetaConfig.NAME)
public final class FSMetaConfig extends AbstractThroughputLatencyJobConfig {
  private static final long serialVersionUID = 7859013978084941882L;

  public static final String NAME = "FSMeta";

  private Command mCommand;
  private int mLevel;
  private int mLevelIgnored;
  private int mDirSize;
  private boolean mUseFileSystemClient;
  private long mBlockSize;
  private long mFileSize;
  private int mReadSize;
  private int mNumReadsPerFile;

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

    /** List status of a file or directory. */
    LIST_STATUS(3),

    /** Random read. */
    // TODO(peis): Consider moving this to a separate test.
    RANDOM_READ(4);

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
   *              {@link FSMetaDefinition#getWorkDir(AbstractThroughputLatencyJobConfig, int, int)}
   *              is level 0.
   * @param levelIgnored the number of levels to ignore (counted from the bottom of the file
   *                     system tree)
   * @param dirSize the number of files or directories each non-leaf directory has
   * @param useFileSystemClient whether to use {@link alluxio.client.file.FileSystem} in
   *                            the benchmark
   * @param parallelism The same task (e.g. list status on a file) will be executed
   *                    multiple times. Note that it is not guaranteed that these duplicate tasks
   *                    will run at the same time.
   * @param expectedThroughput the expected throughput
   * @param writeType the Alluxio file write type
   * @param workDir the working directory
   * @param local whether to run the tasks with locality
   * @param fileSystemType the file system type
   * @param shuffleLoad whether to shuffle the load
   * @param blockSize the blockSize to use if we create a non-empty file
   * @param readSize the number of bytes to read if applicable
   * @param numReadsPerFile the number of read operations to execute per file if applicable
   * @param fileSize the fileSize
   * @param threadNum the number of client threads
   * @param cleanUp whether to clean up after the test
   */
  public FSMetaConfig(@JsonProperty("command") String command, @JsonProperty("level") int level,
      @JsonProperty("levelIgnored") int levelIgnored, @JsonProperty("dirSize") int dirSize,
      @JsonProperty("useFS") boolean useFileSystemClient,
      @JsonProperty("parallelism") int parallelism,
      @JsonProperty("throughput") double expectedThroughput,
      @JsonProperty("writeType") String writeType,
      @JsonProperty("workDir") String workDir,
      @JsonProperty("local") boolean local,
      @JsonProperty("fileSystemType") String fileSystemType,
      @JsonProperty("shuffleLoad") boolean shuffleLoad,
      @JsonProperty("blockSize") String blockSize,
      @JsonProperty("readSize") int readSize,
      @JsonProperty("numReadsPerFile") int numReadsPerFile,
      @JsonProperty("fileSize") String fileSize,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("cleanUp") boolean cleanUp) {
    super(writeType, (int) Math.round(Math.pow(dirSize, level)), parallelism,
        expectedThroughput, workDir, local, threadNum, fileSystemType, shuffleLoad, true, cleanUp);
    mCommand = Command.valueOf(command);
    mDirSize = dirSize;
    mLevel = level;
    mLevelIgnored = levelIgnored;
    mUseFileSystemClient = useFileSystemClient;
    Preconditions.checkState(mLevelIgnored < mLevel);
    mFileSize = FormatUtils.parseSpaceSize(fileSize);
    mBlockSize = FormatUtils.parseSpaceSize(blockSize);
    mReadSize = readSize;
    mNumReadsPerFile = numReadsPerFile;
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
   * @return true if the benchmark uses {@link alluxio.client.file.FileSystem} directly
   */
  public boolean isUseFileSystemClient() {
    return mUseFileSystemClient;
  }

  /**
   * @return the block size
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return the file size
   */
  public long getFileSize() {
    return mFileSize;
  }

  /**
   * @return the read size
   */
  public int getReadSize() {
    return mReadSize;
  }

  /**
   * @return the number of read operations per file if applicable
   */
  public int getNumReadsPerFile() {
    return mNumReadsPerFile;
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
    sb.append(" blockSize: ");
    sb.append(mBlockSize);
    sb.append(" fileSize: ");
    sb.append(mFileSize);
    sb.append(" readSize");
    sb.append(mReadSize);
    sb.append(" numReadsPerFile");
    sb.append(mNumReadsPerFile);
    return sb.toString();
  }
}
