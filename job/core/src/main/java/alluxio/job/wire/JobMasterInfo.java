/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.wire;

import com.google.common.base.Objects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about the Alluxio job master.
 */
@NotThreadSafe
public final class JobMasterInfo implements Serializable {
  private static final long serialVersionUID = 5846173765139223974L;

  private int mWebPort;

  /**
   * Creates a {@link JobMasterInfo} with all fields set to default values.
   */
  public JobMasterInfo() {}

  /**
   * @return the job master web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * @param webPort the web port to set
   * @return the updated master info object
   */
  public JobMasterInfo setWebPort(int webPort) {
    mWebPort = webPort;
    return this;
  }

  /**
   * @return thrift representation of the job master information
   */
  public alluxio.thrift.JobMasterInfo toThrift() {
    return new alluxio.thrift.JobMasterInfo().setWebPort(mWebPort);
  }

  /**
   * @param info the thrift master info to create a wire master info from
   * @return the wire type version of the master info
   */
  public static JobMasterInfo fromThrift(alluxio.thrift.JobMasterInfo info) {
    return new JobMasterInfo().setWebPort(info.getWebPort());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobMasterInfo)) {
      return false;
    }
    JobMasterInfo that = (JobMasterInfo) o;
    return Objects.equal(mWebPort, that.mWebPort);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mWebPort);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("webPort", mWebPort)
        .toString();
  }

  /**
   * Enum representing the fields of the job master info.
   */
  public static enum JobMasterInfoField {
    WEB_PORT;

    /**
     * @return the thrift representation of this job master info field
     */
    public alluxio.thrift.JobMasterInfoField toThrift() {
      return alluxio.thrift.JobMasterInfoField.valueOf(name());
    }

    /**
     * @param field the thrift representation of the job master info field to create
     * @return the wire type version of the job master info field
     */
    public static JobMasterInfoField fromThrift(alluxio.thrift.JobMasterInfoField field) {
      return JobMasterInfoField.valueOf(field.name());
    }
  }
}
