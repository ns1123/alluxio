/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RestUtils;
import alluxio.RuntimeConstants;
import alluxio.web.JobMasterWebServer;
import alluxio.wire.AlluxioJobMasterInfo;

import com.qmino.miredot.annotations.ReturnType;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for requesting general job master information.
 */
@NotThreadSafe
@Path(AlluxioJobMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioJobMasterRestServiceHandler {
  public static final String SERVICE_PREFIX = "job_master";

  // endpoints
  public static final String GET_INFO = "info";

  // queries
  public static final String QUERY_RAW_CONFIGURATION = "raw_configuration";

  private final JobMasterProcess mJobMaster;

  /**
   * @param context context for the servlet
   */
  public AlluxioJobMasterRestServiceHandler(@Context ServletContext context) {
    mJobMaster = (JobMasterProcess) context
        .getAttribute(JobMasterWebServer.ALLUXIO_JOB_MASTER_SERVLET_RESOURCE_KEY);
  }

  /**
   * @summary get the Alluxio job master information
   * @param rawConfiguration if it's true, raw configuration values are returned,
   *    otherwise, they are looked up; if it's not provided in URL queries, then
   *    it is null, which means false.
   * @return the response object
   */
  @GET
  @Path(GET_INFO)
  @ReturnType("alluxio.wire.AlluxioJobMasterInfo")
  public Response getInfo(@QueryParam(QUERY_RAW_CONFIGURATION) final Boolean rawConfiguration) {
    // TODO(jiri): Add a mechanism for retrieving only a subset of the fields.
    return RestUtils.call(new RestUtils.RestCallable<AlluxioJobMasterInfo>() {
      @Override
      public AlluxioJobMasterInfo call() throws Exception {
        boolean rawConfig = false;
        if (rawConfiguration != null) {
          rawConfig = rawConfiguration;
        }
        AlluxioJobMasterInfo result =
            new AlluxioJobMasterInfo()
                .setConfiguration(getConfigurationInternal(rawConfig))
                .setStartTimeMs(mJobMaster.getStartTimeMs())
                .setUptimeMs(mJobMaster.getUptimeMs())
                .setVersion(RuntimeConstants.VERSION)
                .setWorkers(mJobMaster.getJobMaster().getWorkerInfoList());
        return result;
      }
    });
  }

  private Map<String, String> getConfigurationInternal(boolean raw) {
    Set<Map.Entry<String, String>> properties = Configuration.toMap().entrySet();
    SortedMap<String, String> configuration = new TreeMap<>();
    for (Map.Entry<String, String> entry : properties) {
      String key = entry.getKey();
      if (PropertyKey.isValid(key)) {
        if (raw) {
          configuration.put(key, entry.getValue());
        } else {
          configuration.put(key, Configuration.get(PropertyKey.fromString(key)));
        }
      }
    }
    return configuration;
  }
}
