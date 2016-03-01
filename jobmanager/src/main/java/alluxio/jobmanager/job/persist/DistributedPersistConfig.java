/*************************************************************************
* Copyright (c) 2016 Alluxio, Inc.  All rights reserved.
*
* This software and all information contained herein is confidential and
* proprietary to Alluxio, and is protected by copyright and other
* applicable laws in the United States and other jurisdictions.  You may
* not use, modify, reproduce, distribute, or disclose this software
* without the express written permission of Alluxio.
**************************************************************************/
package alluxio.jobmanager.job.persist;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import alluxio.AlluxioURI;
import alluxio.jobmanager.job.JobConfig;

public class DistributedPersistConfig implements JobConfig {
  public static final String NAME = "DistributedPersist";

  private AlluxioURI mFilePath;
  private String mUnderFsPath;

  public DistributedPersistConfig(AlluxioURI filePath, String underFsPath) {
    mFilePath = Preconditions.checkNotNull(filePath);
    mUnderFsPath = Preconditions.checkNotNull(underFsPath);
  }

  public AlluxioURI getFilePath() {
    return mFilePath;
  }

  public String getUnderFsPath() {
    return mUnderFsPath;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("FileURI", mFilePath).add("underFsPath", mUnderFsPath)
        .toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
