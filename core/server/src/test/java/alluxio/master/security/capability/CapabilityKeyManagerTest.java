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

package alluxio.master.security.capability;

import alluxio.master.block.BlockMaster;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Unit tests for {@link CapabilityKeyManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMaster.class)
@PowerMockIgnore({"javax.crypto.*", "javax.net.ssl.*"})
public class CapabilityKeyManagerTest {
  private static final long KEY_LIFETIME_MS = 100L;

  @Test
  public void basic() {
    CapabilityKeyManager manager = new CapabilityKeyManager(0L, KEY_LIFETIME_MS, null);
    Assert.assertEquals(KEY_LIFETIME_MS, manager.getKeyLifetimeMs());
    Assert.assertEquals(16, manager.getCapabilityKey().getEncodedKey().length);
  }

  @Test
  public void keyRotation() {
    BlockMaster blockMaster = Mockito.mock(BlockMaster.class);
    Mockito.when(blockMaster.getWorkerInfoList()).thenReturn(new ArrayList<WorkerInfo>() {});
    CapabilityKeyManager manager = new CapabilityKeyManager(0L, KEY_LIFETIME_MS, blockMaster);
    Assert.assertEquals(0L, manager.getCapabilityKey().getKeyId());
    byte[] oldSecretKey = manager.getCapabilityKey().getEncodedKey();
    CommonUtils.sleepMs(KEY_LIFETIME_MS);
    Assert.assertEquals(1L, manager.getCapabilityKey().getKeyId());
    byte[] newSecretKey = manager.getCapabilityKey().getEncodedKey();
    Assert.assertFalse(Arrays.equals(oldSecretKey, newSecretKey));
  }

  // TODO(chaomin): add more tests for the key rotation logic
}
