/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.worker.job;

import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient.Factory;

/**
 * Extension of MasterClientConfig with defaults that make sense for job master clients.
 */
public class JobMasterClientConfig extends MasterClientConfig {

  /**
   * @return a master client configuration with default values
   */
  public static JobMasterClientConfig defaults() {
    JobMasterClientConfig conf = new JobMasterClientConfig();
    conf.withMasterInquireClient(Factory.createForJobMaster());
    return conf;
  }
}
