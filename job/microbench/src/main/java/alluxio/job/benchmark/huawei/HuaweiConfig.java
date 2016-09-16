/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.huawei;

import alluxio.job.benchmark.AbstractBenchmarkJobConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;

/**
 * The benchmark configuration for Huawei test.
 */
@JsonTypeName(HuaweiConfig.NAME)
public class HuaweiConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 7162741309362058756L;
  public static final String NAME = "HUAWEI";

  private Operation mOperation;
  private int mDepth;
  private int mWidth;
  private int mCount;
  private int mSize;

  /**
   * The huawei test operations supported.
   */
  public enum Operation {
    // Read files.
    READ(0),
    // Alluxio write with write type set to CACHE_THROUGH.
    SYNC_WRITE(1),
    // Alluxio write with write type set to ASYNC_THROUGH.
    ASYNC_WRITE(2),

    DELETE(3);

    private int mValue;

    Operation(int value) {
      mValue = value;
    }

    int getValue() {
      return mValue;
    }
  }

  /**
   * Creates a new instance of {@link HuaweiConfig}.
   * @param operation the operation to test
   * @param depth the depth of the file system to test
   * @param width the number of files in a directory
   * @param count the number of files in total
   * @param size the file size
   * @param cleanUp run clean up after test if set to true
   */
  public HuaweiConfig(@JsonProperty("operation") String operation, @JsonProperty("depth") int depth,
      @JsonProperty("width") int width, @JsonProperty("count") int count,
      @JsonProperty("size") int size, @JsonProperty("cleanUp") boolean cleanUp) {
    super(1, 1, "ALLUXIO", true, cleanUp);
    mOperation = Operation.valueOf(operation);
    mDepth = depth;
    mWidth = width;
    mCount = count;
    mSize = size;
  }

  /**
   * @return the operation to test
   */
  public Operation getOperation() {
    return mOperation;
  }

  /**
   * @return the file system depth
   */
  public int getDepth() {
    return mDepth;
  }

  /**
   * @return return the file system width (i.e. number of files/dirs in a directory)
   */
  public int getWidth() {
    return mWidth;
  }

  /**
   * @return the number of files
   */
  public int getCount() {
    return mCount;
  }

  /**
   * @return file size
   */
  public int getSize() {
    return mSize;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("operation", getOperation())
        .add("depth", getDepth())
        .add("width", getWidth())
        .add("count", getCount())
        .add("size", getSize())
        .add("cleanUp", isCleanUp())
        .toString();
  }
}
