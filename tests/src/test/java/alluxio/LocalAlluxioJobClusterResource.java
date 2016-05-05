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

package alluxio;

import alluxio.exception.ConnectionFailedException;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.LocalAlluxioJobCluster;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A JUnit Rule resource for automatically managing a local alluxio job cluster for testing. To
 * use it, create an instance of the class under a {@literal@}Rule annotation, with the required
 * configuration parameters, and any necessary explicit {@link Configuration} settings. The Alluxio
 * job cluster will be set up from scratch at the end of every method (or at the start of every
 * suite if {@literal@}ClassRule is used), and destroyed at the end.
 */
@NotThreadSafe
public final class LocalAlluxioJobClusterResource implements TestRule {
  /** The Alluxio cluster being managed. */
  private LocalAlluxioJobCluster mLocalAlluxioJobCluster = null;
  /** The {@link Configuration} object used by the cluster. */
  private Configuration mTestConf = null;

  /**
   * Creates a new instance of {@link LocalAlluxioJobClusterResource}.
   */
  public LocalAlluxioJobClusterResource(Configuration conf) {
    mTestConf = conf;
  }

  /**
   * @return the {@link LocalAlluxioJobCluster} being managed
   */
  public LocalAlluxioJobCluster get() {
    return mLocalAlluxioJobCluster;
  }

  /**
   * @return the {@link Configuration} object used by the cluster
   */
  public Configuration getTestConf() {
    return mTestConf;
  }

  /**
   * Explicitly starts the {@link LocalAlluxioCluster}.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public void start() throws IOException, ConnectionFailedException {
    mLocalAlluxioJobCluster.start();
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster(mTestConf);
    try {
      boolean startCluster = true;
      Annotation configAnnotation = description.getAnnotation(Config.class);
      if (configAnnotation != null) {
        Config config = (Config) configAnnotation;
        // Override the configuration parameters with any configuration params
        for (int i = 0; i < config.confParams().length; i += 2) {
          mTestConf.set(config.confParams()[i], config.confParams()[i + 1]);
        }
        // Override startCluster
        startCluster = config.startCluster();
      }
      if (startCluster) {
        mLocalAlluxioJobCluster.start();
      }
    } catch (IOException | ConnectionFailedException e) {
      throw new RuntimeException(e);
    }
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          statement.evaluate();
        } finally {
          mLocalAlluxioJobCluster.stop();
        }
      }
    };
  }

  /**
   * An annotation for test methods that can be used to override any of the defaults set at
   * construction time.
   */
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Config {
    String[] confParams() default {};
    boolean startCluster() default true;
  }
}
