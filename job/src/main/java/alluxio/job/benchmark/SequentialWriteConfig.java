package alluxio.job.benchmark;

import alluxio.client.WriteType;
import alluxio.util.FormatUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SequentialWriteConfig extends AbstractBenchmarkJobConfig {
  public static final String NAME = "SequentialWrite";

  private final long mFileSize;
  private final long mBlockSize;
  private final WriteType mWriteType;
  private final int mBatchSize;

  public SequentialWriteConfig(@JsonProperty("batchNum") int batchNum,
      @JsonProperty("fileSize") String fileSize,
      @JsonProperty("bufferSize") String bufferSize,
      @JsonProperty("blockSize") String blockSize,
      @JsonProperty("writeType") String writeType,
      @JsonProperty("batchSize") int batchSize) {
    // Sequential writes should only use 1 thread.
    super(1, batchNum);
    mFileSize = FormatUtils.parseSpaceSize(fileSize);
    mBlockSize = FormatUtils.parseSpaceSize(blockSize);
    mWriteType = WriteType.valueOf(writeType);
    mBatchSize = batchSize;
  }

  public long getFileSize() {
    return mFileSize;
  }

  public long getBlockSize() {
    return mBlockSize;
  }

  public WriteType getWriteType() {
    return mWriteType;
  }

  public int getBatchSize() {
    return mBatchSize;
  }

  @Override
  public String getName() {
    return NAME;
  }
}