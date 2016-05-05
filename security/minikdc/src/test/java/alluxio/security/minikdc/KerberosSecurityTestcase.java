/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
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
