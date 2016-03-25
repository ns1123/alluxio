/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.Version;
import alluxio.master.block.BlockMaster;
import alluxio.underfs.UnderFileSystem;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for requesting general master information.
 */
@NotThreadSafe
@Path(AlluxioMasterRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class AlluxioMasterRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String ALLUXIO_CONF_PREFIX = "alluxio";

  public static final String SERVICE_PREFIX = "master";
  public static final String GET_RPC_ADDRESS = "rpc_address";
  public static final String GET_CONFIGURATION = "configuration";
  public static final String GET_CAPACITY_BYTES = "capacity_bytes";
  public static final String GET_USED_BYTES = "used_bytes";
  public static final String GET_FREE_BYTES = "free_bytes";
  public static final String GET_CAPACITY_BYTES_ON_TIERS = "capacity_bytes_on_tiers";
  public static final String GET_USED_BYTES_ON_TIERS = "used_bytes_on_tiers";
  public static final String GET_UFS_CAPACITY_BYTES = "ufs_capacity_bytes";
  public static final String GET_UFS_USED_BYTES = "ufs_used_bytes";
  public static final String GET_UFS_FREE_BYTES = "ufs_free_bytes";
  public static final String GET_METRICS = "metrics";
  public static final String GET_START_TIME_MS = "start_time_ms";
  public static final String GET_UPTIME_MS = "uptime_ms";
  public static final String GET_VERSION = "version";
  public static final String GET_WORKER_COUNT = "worker_count";
  public static final String GET_WORKER_INFO_LIST = "worker_info_list";

  private final AlluxioMaster mMaster = AlluxioMaster.get();
  private final BlockMaster mBlockMaster = mMaster.getBlockMaster();
  private final Configuration mMasterConf = MasterContext.getConf();
  private final String mUfsRoot = mMasterConf.get(Constants.UNDERFS_ADDRESS);
  private final UnderFileSystem mUfs = UnderFileSystem.get(mUfsRoot, mMasterConf);

  /**
   * @return the configuration map, the keys are ordered alphabetically
   */
  @GET
  @Path(GET_CONFIGURATION)
  public Response getConfiguration() {
    Set<Map.Entry<Object, Object>> properties = mMasterConf.getInternalProperties().entrySet();
    SortedMap<String, String> configuration = new TreeMap<>();
    for (Map.Entry<Object, Object> entry : properties) {
      String key = entry.getKey().toString();
      if (key.startsWith(ALLUXIO_CONF_PREFIX)) {
        configuration.put(key, (String) entry.getValue());
      }
    }
    return Response.ok(configuration).build();
  }

  /**
   * @return the master rpc address
   */
  @GET
  @Path(GET_RPC_ADDRESS)
  public Response getRpcAddress() {
    return Response.ok(mMaster.getMasterAddress().toString()).build();
  }

  /**
   * @return the master metrics, the keys are ordered alphabetically
   */
  @GET
  @Path(GET_METRICS)
  public Response getMetrics() {
    MetricRegistry metricRegistry = MasterContext.getMasterSource().getMetricRegistry();

    // Get all counters.
    Map<String, Counter> counters = metricRegistry.getCounters();

    // Only the gauge for pinned files is retrieved here, other gauges are statistics of free/used
    // spaces, those statistics can be gotten via other REST apis.
    String filesPinnedProperty = MetricRegistry.name("FilesPinned");
    Gauge filesPinned = metricRegistry.getGauges().get(filesPinnedProperty);

    // Get values of the counters and gauges and put them into a metrics map.
    SortedMap<String, Long> metrics = new TreeMap<>();
    for (Map.Entry<String, Counter> counter : counters.entrySet()) {
      metrics.put(counter.getKey(), counter.getValue().getCount());
    }
    metrics.put(filesPinnedProperty, ((Integer) filesPinned.getValue()).longValue());

    return Response.ok(metrics).build();
  }

  /**
   * @return the start time of the master
   */
  @GET
  @Path(GET_START_TIME_MS)
  public Response getStartTimeMs() {
    return Response.ok(mMaster.getStartTimeMs()).build();
  }

  /**
   * @return the uptime of the master
   */
  @GET
  @Path(GET_UPTIME_MS)
  public Response getUptimeMs() {
    return Response.ok(mMaster.getUptimeMs()).build();
  }

  /**
   * @return the version of the master
   */
  @GET
  @Path(GET_VERSION)
  public Response getVersion() {
    return Response.ok(Version.VERSION).build();
  }

  /**
   * @return the total capacity of all workers in bytes
   */
  @GET
  @Path(GET_CAPACITY_BYTES)
  public Response getCapacityBytes() {
    return Response.ok(mBlockMaster.getCapacityBytes()).build();
  }

  /**
   * @return the used capacity
   */
  @GET
  @Path(GET_USED_BYTES)
  public Response getUsedBytes() {
    return Response.ok(mBlockMaster.getUsedBytes()).build();
  }

  /**
   * @return the free capacity
   */
  @GET
  @Path(GET_FREE_BYTES)
  public Response getFreeBytes() {
    return Response.ok(mBlockMaster.getCapacityBytes() - mBlockMaster.getUsedBytes()).build();
  }

  /**
   * @return the total ufs capacity in bytes, a negative value means the capacity is unknown
   */
  @GET
  @Path(GET_UFS_CAPACITY_BYTES)
  public Response getUfsCapacityBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_TOTAL)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @return the used disk capacity, a negative value means the capacity is unknown
   */
  @GET
  @Path(GET_UFS_USED_BYTES)
  public Response getUfsUsedBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_USED)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  /**
   * @return the free ufs capacity in bytes, a negative value means the capacity is unknown
   */
  @GET
  @Path(GET_UFS_FREE_BYTES)
  public Response getUfsFreeBytes() {
    try {
      return Response.ok(mUfs.getSpace(mUfsRoot, UnderFileSystem.SpaceType.SPACE_FREE)).build();
    } catch (IOException e) {
      LOG.warn(e.getMessage());
      return Response.serverError().entity(e.getMessage()).build();
    }
  }

  private Comparator<String> getTierAliasComparator() {
    return new Comparator<String>() {
      private MasterStorageTierAssoc mTierAssoc = new MasterStorageTierAssoc(mMasterConf);

      @Override
      public int compare(String tier1, String tier2) {
        int ordinal1 = mTierAssoc.getOrdinal(tier1);
        int ordinal2 = mTierAssoc.getOrdinal(tier2);
        if (ordinal1 < ordinal2) {
          return -1;
        }
        if (ordinal1 == ordinal2) {
          return 0;
        }
        return 1;
      }
    };
  }

  /**
   * @return the mapping from tier alias to total capacity of the tier in bytes, keys are in the
   *     order from tier alias with smaller ordinal to those with larger ones
   */
  @GET
  @Path(GET_CAPACITY_BYTES_ON_TIERS)
  public Response getCapacityBytesOnTiers() {
    SortedMap<String, Long> capacityBytesOnTiers = new TreeMap<>(getTierAliasComparator());
    for (Map.Entry<String, Long> tierBytes : mBlockMaster.getTotalBytesOnTiers().entrySet()) {
      capacityBytesOnTiers.put(tierBytes.getKey(), tierBytes.getValue());
    }
    return Response.ok(capacityBytesOnTiers).build();
  }

  /**
   * @return the mapping from tier alias to the used bytes of the tier, keys are in the order
   *     from tier alias with smaller ordinal to those with larger ones
   */
  @GET
  @Path(GET_USED_BYTES_ON_TIERS)
  public Response getUsedBytesOnTiers() {
    SortedMap<String, Long> usedBytesOnTiers = new TreeMap<>(getTierAliasComparator());
    for (Map.Entry<String, Long> tierBytes : mBlockMaster.getUsedBytesOnTiers().entrySet()) {
      usedBytesOnTiers.put(tierBytes.getKey(), tierBytes.getValue());
    }
    return Response.ok(usedBytesOnTiers).build();
  }

  /**
   * @return the count of workers
   */
  @GET
  @Path(GET_WORKER_COUNT)
  public Response getWorkerCount() {
    return Response.ok(mBlockMaster.getWorkerCount()).build();
  }

  /**
   * @return the list of worker descriptors
   */
  @GET
  @Path(GET_WORKER_INFO_LIST)
  public Response getWorkerInfoList() {
    return Response.ok(mBlockMaster.getWorkerInfoList()).build();
  }
}
