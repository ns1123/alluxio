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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import alluxio.Constants;
import alluxio.jobmanager.job.JobDefinition;
import alluxio.jobmanager.job.JobMasterContext;
import alluxio.jobmanager.job.JobWorkerContext;
import alluxio.wire.WorkerInfo;

public class DistributedPersistDefinition
    implements JobDefinition<DistributedPersistConfig, List<Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public Map<WorkerInfo, List<Long>> selectExecutors(DistributedPersistConfig config,
      List<WorkerInfo> workerInfoList, JobMasterContext jobMasterContext) {
    Map<WorkerInfo, List<Long>> map = Maps.newHashMap();
    map.put(workerInfoList.get(0), Lists.newArrayList(1L, 2L));
    return map;
  }

  @Override
  public void runTask(DistributedPersistConfig config, List<Long> args,
      JobWorkerContext jobWorkerContext) throws Exception {
    LOG.info("running DistributedPersist with args:" + args);
    throw new UnsupportedOperationException();
  }
}
