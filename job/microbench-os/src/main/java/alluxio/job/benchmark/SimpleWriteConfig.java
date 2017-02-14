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
import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * The configuration for the SimpleWrite benchmark job.
 */
@JsonTypeName(SimpleWriteConfig.NAME)
public class SimpleWriteConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 8696209904079086810L;
  public static final String NAME = "SimpleWrite";
  public static final String READ_WRITE_DIR = "/simple-read-write/";

  private long mBlockSize;
  private String mFileSize;
  private String mBufferSize;
  private String mBaseDir;
  private WriteType mWriteType;
  private short mHdfsReplication;

  // TODO(chaomin): merge writeType with fileSysmteType.
  /**
   * Creates a new instance of {@link SimpleWriteConfig}.
   *
   * @param blockSize the block size
   * @param bufferSize the buffer size
   * @param cleanUp whether to cleanup the state after the test
   * @param fileSize the file size
   * @param fileSystemType the file system type
   * @param hdfsReplication the replication refactor for HDFS file
   * @param threadNum the thread number
   * @param writeType the write type
   * @param baseDir the base directory for the test files
   * @param verbose whether the report is verbose
   */
  public SimpleWriteConfig(
      @JsonProperty("blockSize") String blockSize,
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("fileSize") String fileSize,
      @JsonProperty("fileSystemType") String fileSystemType,
      @JsonProperty("hdfsReplication") int hdfsReplication,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("writeType") String writeType,
      @JsonProperty("baseDir") String baseDir,
      @JsonProperty("verbose") boolean verbose,
      @JsonProperty("cleanUp") boolean cleanUp) {
    super(threadNum, 1, fileSystemType, verbose, cleanUp);
    Preconditions.checkNotNull(blockSize, "block size cannot be null");
    Preconditions.checkNotNull(bufferSize, "buffer size cannot be null");
    Preconditions.checkNotNull(fileSize, "file size cannot be null");
    Preconditions.checkNotNull(writeType, "the write type cannot be null");
    // validate the input to fail fast
    FormatUtils.parseSpaceSize(fileSize);
    mFileSize = fileSize;
    FormatUtils.parseSpaceSize(bufferSize);
    mBufferSize = bufferSize;
    FormatUtils.parseSpaceSize(blockSize);
    mBlockSize = FormatUtils.parseSpaceSize(blockSize);
    mWriteType = WriteType.valueOf(writeType);
    // Set default HDFS replication factor to 1 for Alluxio benchmark purpose.
    mHdfsReplication = hdfsReplication > 0 ? (short) hdfsReplication : 1;
    mBaseDir = baseDir != null ? baseDir : READ_WRITE_DIR;
  }

  /**
   * @return the block size
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return the buffer size
   */
  public String getBufferSize() {
    return mBufferSize;
  }

  /**
   * @return the file size
   */
  public String getFileSize() {
    return mFileSize;
  }

  /**
   * @return the write type
   */
  public WriteType getWriteType() {
    return mWriteType;
  }

  /**
   * @return the HDFS replication factor
   */
  public short getHdfsReplication() {
    return mHdfsReplication;
  }

  /**
   * @return the base directory for the test files
   */
  public String getBaseDir() {
    return mBaseDir;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("batchSize", getBatchNum())
        .add("blockSize", mBlockSize)
        .add("bufferSize", mBufferSize)
        .add("cleanUp", isCleanUp())
        .add("fileSize", mFileSize)
        .add("fileSystemType", getFileSystemType().toString())
        .add("hdfsReplication", getHdfsReplication())
        .add("threadNum", getThreadNum())
        .add("baseDir", getBaseDir())
        .add("verbose", isVerbose())
        .add("writeType", mWriteType)
        .add("cleanUp", isCleanUp())
        .toString();
  }
}
