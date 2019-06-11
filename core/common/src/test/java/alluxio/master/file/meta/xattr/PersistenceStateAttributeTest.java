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

package alluxio.master.file.meta.xattr;

import static alluxio.master.file.meta.xattr.ExtendedAttribute.PERSISTENCE_STATE;
import static org.junit.Assert.assertEquals;

import alluxio.master.file.meta.PersistenceState;

import org.junit.Test;

public class PersistenceStateAttributeTest {

  @Test
  public void testSingleEncode() {
    for (int i = 0; i < PersistenceState.values().length; i++) {
      PersistenceState state = PersistenceState.values()[i];
      assertEquals(state, PERSISTENCE_STATE.decode(
          PERSISTENCE_STATE.encode(state)));
    }
  }
}
