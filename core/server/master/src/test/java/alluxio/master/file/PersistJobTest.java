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

package alluxio.master.file;

import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for {@link PersistJob}.
 */
public final class PersistJobTest {

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    long fileId = random.nextLong();
    long jobId = random.nextLong();
    String tempUfsPath = CommonUtils.randomAlphaNumString(random.nextInt(10));

    PersistJob persistJob = new PersistJob(fileId, jobId, tempUfsPath);

    Assert.assertEquals(fileId, persistJob.getFileId());
    Assert.assertEquals(jobId, persistJob.getJobId());
    Assert.assertEquals(tempUfsPath, persistJob.getTempUfsPath());
    Assert.assertEquals(PersistJob.CancelState.NOT_CANCELED, persistJob.getCancelState());
  }

  /**
   * Tests getting and setting the cancel state.
   */
  @Test
  public void cancelState() {
    PersistJob persistJob = new PersistJob(0, 0, "");
    persistJob.setCancelState(PersistJob.CancelState.NOT_CANCELED);
    Assert.assertEquals(PersistJob.CancelState.NOT_CANCELED, persistJob.getCancelState());
    persistJob.setCancelState(PersistJob.CancelState.TO_BE_CANCELED);
    Assert.assertEquals(PersistJob.CancelState.TO_BE_CANCELED, persistJob.getCancelState());
    persistJob.setCancelState(PersistJob.CancelState.CANCELING);
    Assert.assertEquals(PersistJob.CancelState.CANCELING, persistJob.getCancelState());
  }
}
