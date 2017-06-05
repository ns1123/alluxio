/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link JobDefinitionRegistry}.
 */
public final class JobDefinitionRegistryTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void getJobDefinitionTest() throws Exception {
    JobDefinition<TestJobConfig, ?, ?> definition = JobDefinitionRegistry.INSTANCE
        .getJobDefinition(new TestJobConfig("test"));
    Assert.assertTrue(definition instanceof TestJobDefinition);
  }

  @Test
  public void getNonexistingJobDefinitionTest() throws Exception {
    DummyJobConfig jobConfig = new DummyJobConfig();

    mThrown.expect(JobDoesNotExistException.class);
    mThrown.expectMessage(
        ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobConfig.getName()));

    JobDefinitionRegistry.INSTANCE.getJobDefinition(jobConfig);
  }

  class DummyJobConfig implements JobConfig {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "dummy";
    }
  }
}
