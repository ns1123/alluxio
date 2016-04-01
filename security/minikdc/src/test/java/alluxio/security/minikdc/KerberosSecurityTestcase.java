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

package alluxio.security.minikdc;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.util.Properties;

/**
 * KerberosSecurityTestcase provides a base class for using MiniKdc with other testcases.
 * KerberosSecurityTestcase starts the MiniKdc (@Before) before running tests, and stop the MiniKdc
 * (@After) after the testcases, using default settings (working dir and mKdc configurations).
 * <p>
 * Users can directly inherit this class and implement their own test functions using the default
 * settings, or override functions getTestDir() and createMiniKdcConf() to provide new settings.
 *
 */
public class KerberosSecurityTestcase {
  private MiniKdc mKdc;
  private File mWorkDir;
  private Properties mConf;

  /**
   * Starts a kdc.
   *
   * @throws Exception thrown if the MiniKdc could not be created
   */
  @Before
  public void startMiniKdc() throws Exception {
    createTestDir();
    createMiniKdcConf();

    mKdc = new MiniKdc(mConf, mWorkDir);
    mKdc.start();
  }

  /**
   * Creates a working directory, it should be the build directory. Under this directory an ApacheDS
   * working directory will be created, this directory will be deleted when the MiniKdc stops.
   */
  public void createTestDir() {
    mWorkDir = new File(System.getProperty("test.dir", "target"));
  }

  /**
   * Creates a Kdc configuration.
   */
  public void createMiniKdcConf() {
    mConf = MiniKdc.createConf();
  }

  /**
   * Stops a mini kdc.
   */
  @After
  public void stopMiniKdc() {
    if (mKdc != null) {
      mKdc.stop();
    }
  }

  /**
   * Gets kdc.
   * @return mKdc
   */
  public MiniKdc getKdc() {
    return mKdc;
  }

  /**
   * Gets workDir.
   * @return mWorkDir
   */
  public File getWorkDir() {
    return mWorkDir;
  }

  /**
   * Gets conf.
   * @return mConf
   */
  public Properties getConf() {
    return mConf;
  }
}
