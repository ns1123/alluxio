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

import static alluxio.master.policy.action.data.DataActionDefinition.Operation.REMOVE;
import static alluxio.master.policy.action.data.DataActionDefinition.Operation.STORE;

import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.xattr.ExtendedAttribute;
import alluxio.master.policy.cond.Condition;
import alluxio.master.policy.cond.ConditionFactory;
import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A condition for testing the state of data.
 */
public class DataState implements Condition {
  private final List<DataActionDefinition.LocationOperation> mOperations;

  /**
   * Factory for creating instances.
   */
  public static class Factory implements ConditionFactory {
    @Override
    public Condition deserialize(String serialized) {
      if (serialized.startsWith("dataState(") && serialized.endsWith(")")) {
        serialized = serialized.substring("dataState(".length(), serialized.length() - 1);
      } else {
        return null;
      }

      List<DataActionDefinition.LocationOperation> ops =
          DataActionUtils.deserializeBody(serialized);
      return new DataState(ops);
    }
  }

  /**
   * Creates an instance of {@link DataState}.
   *
   * @param operations the list of {@link DataActionDefinition.LocationOperation}
   */
  public DataState(List<DataActionDefinition.LocationOperation> operations) {
    mOperations = operations;
  }

  /**
   * Returns {@link IntervalSet#ALWAYS} or {@link IntervalSet#NEVER} depending on whether the
   * the inodeState for the given path already exists in the state that the operations for this
   * {@link DataState} desire.
   *
   * For all operations, if there are any in which the state of the path is already in the state
   * that an operation desires, we return {@link IntervalSet#ALWAYS}; otherwise,
   * {@link IntervalSet#NEVER}
   *
   * @param interval the time interval to consider the condition for
   * @param path the Alluxio path to evaluate the condition for
   * @param state the state of the path
   * @return {@link IntervalSet#ALWAYS} or {@link IntervalSet#NEVER}
   */
  @Override
  public IntervalSet evaluate(Interval interval, String path, InodeState state) {
    Map<String, byte[]> attr = state.getXAttr();
    // Use an empty hash map in case xattr isn't defined
    attr = attr == null ? Collections.emptyMap() : attr;

    boolean persisted = state.isPersisted();
    for (DataActionDefinition.LocationOperation o : mOperations) {
      // TODO(zac) implement evaluate for non-UFS functions
      if (o.getLocation() != DataActionDefinition.Location.UFS) {
        continue;
      }

      String modifier = o.getLocationModifier();
      String k = modifier.equals("") ? null :
          ExtendedAttribute.PERSISTENCE_STATE.forId(o.getLocationModifier());

      // The attribute may or may not be in the map
      // If the attribute isn't in the map, we assume the state to be NOT_PERSISTED because it is
      // safer to assume the data isn't there. Store or REMOVE operations will still end up with
      // the UFS in the same state if the original state is assumed to be NOT_PERSISTED. Otherwise,
      // if it is in the map, we must take into account the state reported by the inode.
      byte[] val = attr.getOrDefault(k, null);
      PersistenceState decoded = val != null ? ExtendedAttribute.PERSISTENCE_STATE.decode(val)
          : PersistenceState.NOT_PERSISTED;

      if (o.getOperation() == STORE
          && (!persisted || (k != null && decoded == PersistenceState.NOT_PERSISTED))) {
        return IntervalSet.NEVER;
      } else if (o.getOperation() == REMOVE
          && (persisted && (k == null || decoded == PersistenceState.PERSISTED))) {
        return IntervalSet.NEVER;
      }
    }
    return new IntervalSet(interval);
  }

  @Override
  public String serialize() {
    return "dataState(" + mOperations.stream().map(DataActionDefinition.LocationOperation::toString)
        .collect(Collectors.joining(", ")) + ")";
  }
}
