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

package alluxio.master.policy.action.data;

import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Tests for {@link DataActionUtils}.
 */
public final class DataActionUtilsTest {
  @Test
  public void serde() throws Exception {
    for (int i = 0; i < 1000; i++) {
      String s = createRandomDefinition();
      List<DataActionDefinition.LocationOperation> ops = DataActionUtils.deserializeBody(s);
      Assert.assertNotNull(ops);
      Assert.assertEquals(s, DataActionUtils.serializeBody(ops));
    }
  }

  @Test
  public void deserializeInvalid() throws Exception {
    invalidDeserialize("DATA(invalid)");
    invalidDeserialize("DATA(ALLUXIO:invalid)");
    invalidDeserialize("DATA(invalid:STORE)");
    invalidDeserialize("DATA(ALLUXIO:STORE, invalid)");
    invalidDeserialize("DATA(invalid, ALLUXIO:STORE)");
    invalidDeserialize("DATA(ALLUXIO:STORE[])");
  }

  private void invalidDeserialize(String s) {
    try {
      DataActionUtils.deserializeBody(s);
      Assert.fail("Expected deserialize to fail for input: " + s);
    } catch (Exception e) {
      // expected
    }
  }

  private String createRandomDefinition() {
    ThreadLocalRandom rand = ThreadLocalRandom.current();

    return rand.longs(rand.nextInt(5)).boxed().map(
        unused -> {
          DataActionDefinition.Location l = DataActionDefinition.Location.values()[rand
              .nextInt(DataActionDefinition.Location.values().length)];
          DataActionDefinition.Operation o = DataActionDefinition.Operation.values()[rand
              .nextInt(DataActionDefinition.Operation.values().length)];

          String location = l.name();
          String modifier = CommonUtils.randomAlphaNumString(rand.nextInt(2));
          if (!modifier.isEmpty()) {
            location += "[" + modifier + "]";
          }

          String operation = o.name();
          modifier = CommonUtils.randomAlphaNumString(rand.nextInt(2));
          if (!modifier.isEmpty()) {
            operation += "[" + modifier + "]";
          }

          return location + ":" + operation;
        }).collect(Collectors.joining(", "));
  }
}
