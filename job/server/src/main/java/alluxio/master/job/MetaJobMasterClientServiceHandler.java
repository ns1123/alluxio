/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.job;

import alluxio.Constants;
import alluxio.master.AlluxioJobMasterService;
import alluxio.thrift.JobMasterInfo;
import alluxio.thrift.JobMasterInfoField;
import alluxio.thrift.MetaJobMasterClientService.Iface;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is a Thrift handler for meta job master RPCs.
 */
public final class MetaJobMasterClientServiceHandler implements Iface {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final AlluxioJobMasterService mAlluxioJobMaster;

  /**
   * @param alluxioJobMaster the Alluxio job master
   */
  public MetaJobMasterClientServiceHandler(AlluxioJobMasterService alluxioJobMaster) {
    mAlluxioJobMaster = alluxioJobMaster;
  }

  @Override
  public JobMasterInfo getInfo(Set<JobMasterInfoField> fields) throws TException {
    if (fields == null) {
      fields = new HashSet<>(Arrays.asList(JobMasterInfoField.values()));
    }
    JobMasterInfo info = new alluxio.thrift.JobMasterInfo();
    for (JobMasterInfoField field : fields) {
      switch (field) {
        case WEB_PORT:
          info.setWebPort(mAlluxioJobMaster.getWebAddress().getPort());
          break;
        default:
          LOG.warn("Unrecognized master info field: " + field);
      }
    }
    return info;
  }

  @Override
  public long getServiceVersion() throws TException {
    return Constants.META_JOB_MASTER_CLIENT_SERVICE_VERSION;
  }
}
