/*************************************************************************
* Copyright (c) 2016 Alluxio, Inc.  All rights reserved.
*
* This software and all information contained herein is confidential and
* proprietary to Alluxio, and is protected by copyright and other
* applicable laws in the United States and other jurisdictions.  You may
* not use, modify, reproduce, distribute, or disclose this software
* without the express written permission of Alluxio.
**************************************************************************/
package alluxio.jobmanager.job;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import alluxio.jobmanager.job.persist.DistributedPersistConfig;
import alluxio.jobmanager.job.prefetch.DistributedPrefetchingConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DistributedPersistConfig.class,
        name = DistributedPersistConfig.NAME),
    @JsonSubTypes.Type(value = DistributedPrefetchingConfig.class,
        name = DistributedPrefetchingConfig.NAME)})
/**
 * A job configuration. All the subclasses are both Java and JSON serializable.
 */
public interface JobConfig extends Serializable {
  /**
   * @return the name of the job
   */
  String getName();
}
