/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.job.benchmark.RemoteReadConfig;
import alluxio.job.benchmark.SequentialWriteConfig;
import alluxio.job.benchmark.SimpleReadConfig;
import alluxio.job.benchmark.SimpleWriteConfig;
import alluxio.job.benchmark.ThroughputLatency;
import alluxio.job.benchmark.ThroughputLatencyJobConfig;
import alluxio.job.load.LoadConfig;
import alluxio.job.move.MoveConfig;
import alluxio.job.persist.PersistConfig;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * A job configuration. All the subclasses are both Java and JSON serializable.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = LoadConfig.class, name = LoadConfig.NAME),
    @JsonSubTypes.Type(value = MoveConfig.class, name = MoveConfig.NAME),
    @JsonSubTypes.Type(value = PersistConfig.class, name = PersistConfig.NAME),
    @JsonSubTypes.Type(value = SimpleWriteConfig.class, name = SimpleWriteConfig.NAME),
    @JsonSubTypes.Type(value = SimpleReadConfig.class, name = SimpleReadConfig.NAME),
    @JsonSubTypes.Type(value = SequentialWriteConfig.class, name = SequentialWriteConfig.NAME),
    @JsonSubTypes.Type(value = RemoteReadConfig.class, name = RemoteReadConfig.NAME),
    @JsonSubTypes.Type(value = ThroughputLatencyJobConfig.class,
        name = ThroughputLatencyJobConfig.NAME)})
public interface JobConfig extends Serializable {
  /**
   * @return the name of the job
   */
  String getName();
}
