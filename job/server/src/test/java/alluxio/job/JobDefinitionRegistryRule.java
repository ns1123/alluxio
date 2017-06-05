/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.powermock.reflect.Whitebox;

import java.util.Map;

/**
 * Rule for adding to the job registry and then cleaning up the change.
 */
public final class JobDefinitionRegistryRule implements TestRule {
  private Class<? extends JobConfig> mConfig;
  private JobDefinition<?, ?, ?> mDefinition;

  /**
   * @param keyValuePairs map from configuration keys to the values to set them to
   */
  public JobDefinitionRegistryRule(Class<? extends JobConfig> config,
      JobDefinition<?, ?, ?> definition) {
    mConfig = config;
    mDefinition = definition;
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        @SuppressWarnings("unchecked")
        Map<Class<?>, JobDefinition<?, ?, ?>> registry =
            Whitebox.getInternalState(JobDefinitionRegistry.INSTANCE, Map.class);
        registry.put(mConfig, mDefinition);
        try {
          statement.evaluate();
        } finally {
          registry.remove(mConfig);
        }
      }
    };
  }
}
