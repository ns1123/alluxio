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

package alluxio.wire;

import alluxio.CommonTestUtils;
import alluxio.util.CommonUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class LicenseInfoTest {

  @Test
  public void json() throws Exception {
    LicenseInfo licenseInfo = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    LicenseInfo other = mapper.readValue(mapper.writeValueAsBytes(licenseInfo), LicenseInfo.class);
    checkEquality(licenseInfo, other);
  }

  @Test
  public void equals() {
    CommonTestUtils.testEquals(AlluxioMasterInfo.class);
  }

  private void checkEquality(LicenseInfo a, LicenseInfo b) {
    Assert.assertEquals(a.getChecksum(), b.getChecksum());
    Assert.assertEquals(a.getName(), b.getName());
    Assert.assertEquals(a.getKey(), b.getKey());
    Assert.assertEquals(a.getEmail(), b.getEmail());
    Assert.assertEquals(a.getLastCheckMs(), b.getLastCheckMs());
    Assert.assertEquals(a.getLastCheckSuccessMs(), b.getLastCheckSuccessMs());
    Assert.assertEquals(a, b);
  }

  protected static LicenseInfo createRandom() {
    LicenseInfo result = new LicenseInfo();
    Random random = new Random();

    String checksum = CommonUtils.randomAlphaNumString(10);
    String name = CommonUtils.randomAlphaNumString(10);
    String key = CommonUtils.randomAlphaNumString(10);
    String email = CommonUtils.randomAlphaNumString(10);
    long lastCheckMs = random.nextLong();
    long lastCheckSuccessMs = random.nextLong();

    result.setChecksum(checksum);
    result.setName(name);
    result.setKey(key);
    result.setEmail(email);
    result.setLastCheckMs(lastCheckMs);
    result.setLastCheckSuccessMs(lastCheckSuccessMs);

    return result;
  }
}
