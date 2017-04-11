/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.client.job;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.resource.ResourcePool;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for managing job master clients. After obtaining a client with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
public final class JobMasterClientPool extends ResourcePool<JobMasterClient> {
  private final Queue<JobMasterClient> mClientList;

  /**
   * Creates a new job master client pool.
   */
  public JobMasterClientPool() {
    super(Configuration.getInt(PropertyKey.JOB_MASTER_CLIENT_THREADS));
    mClientList = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void close() {
    JobMasterClient client;
    while ((client = mClientList.poll()) != null) {
      client.close();
    }
  }

  @Override
  protected JobMasterClient createNewResource() {
    JobMasterClient client = JobMasterClient.Factory.create();
    mClientList.add(client);
    return client;
  }
}
