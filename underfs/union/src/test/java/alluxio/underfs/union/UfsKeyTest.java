/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import alluxio.ConfigurationTestUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.mock.MockUnderFileSystem;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

public class UfsKeyTest {

  @Test
  public void UfsKeyEquals() {

    // Ufs typically don't implement {@link #Object#equals}. So, for this test, we assume they are
    // only compared by pointer.
    UnderFileSystemConfiguration ufsConf =
        UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults());
    UnderFileSystem ufs = new MockUnderFileSystem(ufsConf);

    new EqualsTester()
        .addEqualityGroup(new UfsKey("A", 1, null), new UfsKey("A", 1, null))
        .addEqualityGroup(new UfsKey("B", 1, null), new UfsKey("B", 1, null))
        .addEqualityGroup(new UfsKey("C", 2, null), new UfsKey("C", 2, null))
        .addEqualityGroup(new UfsKey("C", 2, ufs), new UfsKey("C", 2, ufs))
        .addEqualityGroup(new UfsKey("C", 4, ufs))
        .testEquals();
  }
}
