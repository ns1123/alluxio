/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.ProcessUtils;
import alluxio.RuntimeConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for the Alluxio master.
 */
@ThreadSafe
public final class AlluxioMaster {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMaster.class);

  /**
   * Starts the Alluxio master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioMaster.class.getCanonicalName());
      System.exit(-1);
    }

<<<<<<< HEAD:core/server/master/src/main/java/alluxio/master/AlluxioMaster.java
    MasterProcess process = MasterProcess.Factory.create();
    ProcessUtils.run(process);
=======
    // ALLUXIO CS ADD
    alluxio.util.CommonUtils.PROCESS_TYPE.set(alluxio.util.CommonUtils.ProcessType.MASTER);
    // ALLUXIO CS END
    AlluxioMasterService master = AlluxioMasterService.Factory.create();
    ServerUtils.run(master, "Alluxio master");
>>>>>>> origin/enterprise-1.4-ts:core/server/src/main/java/alluxio/master/AlluxioMaster.java
  }

  private AlluxioMaster() {} // prevent instantiation
}
