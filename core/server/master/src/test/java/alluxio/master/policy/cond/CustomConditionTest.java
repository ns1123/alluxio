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

package alluxio.master.policy.cond;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Tests for custom {@link Condition}.
 */
public final class CustomConditionTest {
  @Test
  public void createOlderThan() {
    testRandomPermutations(getOlderThanList());
  }

  @Test
  public void createXAttr() {
    testRandomPermutations(getXAttrList());
  }

  @Test
  public void createDataState() {
    testRandomPermutations(getDataState());
  }

  @Test
  public void createRandom() {
    List<String> conds = new ArrayList<>();
    conds.addAll(getOlderThanList());
    conds.addAll(getXAttrList());
    conds.addAll(getDataState());
    testRandomPermutations(conds);
  }

  private void testRandomPermutations(List<String> conds) {
    for (int i = 0; i < 1000; i++) {
      Collections.shuffle(conds);
      String conditionSerialized = generateRandomPermutation(conds);
      Condition condition = Condition.deserialize(conditionSerialized);
      Assert.assertNotNull(condition);

      String doubleSerialized = condition.serialize();
      Assert.assertNotNull(doubleSerialized);

      Assert.assertNotNull(Condition.deserialize(doubleSerialized));
    }
  }

  private String generateRandomPermutation(List<String> conds) {
    List<String> subConds =
        conds.subList(0, ThreadLocalRandom.current().nextInt(1, conds.size()));
    String conditionSerialized = subConds.stream().map(
        c -> {
          if (ThreadLocalRandom.current().nextBoolean()) {
            return "NOT(" + c + ")";
          }
          return c;
        }).collect(Collectors.joining(" AND "));
    if (ThreadLocalRandom.current().nextBoolean()) {
      conditionSerialized = "NOT(" + conditionSerialized + ")";
    }
    return conditionSerialized;
  }

  private List<String> getOlderThanList() {
    return Arrays.asList(
        "olderThan(11)",
        "olderThan(22ms)",
        "olderThan(33s)",
        "olderThan(44m)",
        "olderThan(55d)",
        "olderThan(66d)"
    );
  }

  private List<String> getXAttrList() {
    return Arrays.asList(
        "xAttr(key, null)",
        "xAttr(a.b.c, \"null\")",
        "xAttr(d.e, \"value\")"
    );
  }

  private List<String> getDataState() {
    return Arrays.asList(
        "dataState()",
        "dataState(ALLUXIO:REMOVE)",
        "dataState(ALLUXIO:STORE)",
        "dataState(UFS:REMOVE)",
        "dataState(UFS:STORE)",
        "dataState(UFS[sub1]:REMOVE)",
        "dataState(UFS[sub2]:STORE)",
        "dataState(ALLUXIO:STORE, UFS:REMOVE, UFS[sub1]:STORE)"
    );
  }
}
