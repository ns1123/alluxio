/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import java.io.Serializable;
import java.util.Map;

/**
 * An abstract job definition where the run task method does not return a value.
 *
 * @param <T> the job configuration type
 * @param <P> the argument type
 */
public abstract class AbstractVoidJobDefinition<T extends JobConfig, P extends Serializable>
    implements JobDefinition<T, P, SerializableVoid> {

  @Override
  public String join(T config, Map<WorkerInfo, SerializableVoid> taskResults) throws Exception {
    return "";
  }
}
