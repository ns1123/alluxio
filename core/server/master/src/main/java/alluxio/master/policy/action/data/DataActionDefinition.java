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

import alluxio.master.policy.action.ActionDefinition;
import alluxio.master.policy.action.ActionDefinitionFactory;
import alluxio.master.policy.action.ActionExecution;
import alluxio.master.policy.action.ActionExecutionContext;
import alluxio.master.policy.meta.InodeState;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * The data action defines how data should be stored within Alluxio and UFS.
 */
public class DataActionDefinition implements ActionDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(DataActionDefinition.class);
  private static final String NAME = "DATA";

  /**
   * These are the locations possible for this action.
   */
  public enum Location {
    ALLUXIO,
    UFS,
  }

  /**
   * These are the operations possible for this action.
   */
  public enum Operation {
    STORE,
    REMOVE,
  }

  /**
   * This represents an operation for a location.
   */
  public static class LocationOperation {
    private final Location mLocation;
    private final String mLocationModifier;
    private final Operation mOperation;
    private final String mOperationModifier;

    /**
     * Creates an instance of {@link LocationOperation}.
     *
     * @param location the location
     * @param locationModifier the (optional) location modifier string
     * @param operation the operation
     * @param operationModifier the (optional) operation modifier string
     */
    public LocationOperation(Location location, @Nullable String locationModifier,
        Operation operation, @Nullable String operationModifier) {
      mLocation = location;
      mLocationModifier = locationModifier != null ? locationModifier : "";
      mOperation = operation;
      mOperationModifier = operationModifier != null ? operationModifier : "";
    }

    /**
     * Creates an instance of {@link LocationOperation} with empty modifiers.
     *
     * @param location the location
     * @param operation the operation
     */
    public LocationOperation(Location location, Operation operation) {
      this(location, null, operation, null);
    }

    /**
     * @return the location
     */
    public Location getLocation() {
      return mLocation;
    }

    /**
     * @return the location modifier
     */
    public String getLocationModifier() {
      return mLocationModifier;
    }

    /**
     * @return the operation
     */
    public Operation getOperation() {
      return mOperation;
    }

    /**
     * @return the operation modifier
     */
    public String getOperationModifier() {
      return mOperationModifier;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder(mLocation.name());
      if (!mLocationModifier.isEmpty()) {
        builder.append('[');
        builder.append(mLocationModifier);
        builder.append(']');
      }
      builder.append(':');
      builder.append(mOperation.name());
      if (!mOperationModifier.isEmpty()) {
        builder.append('[');
        builder.append(mOperationModifier);
        builder.append(']');
      }
      return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LocationOperation)) {
        return false;
      }
      LocationOperation operation = (LocationOperation) o;
      return mLocation == operation.mLocation
          && Objects.equals(mLocationModifier, operation.mLocationModifier)
          && mOperation == operation.mOperation
          && Objects.equals(mOperationModifier, operation.mOperationModifier);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mLocation, mLocationModifier, mOperation, mOperationModifier);
    }
  }

  /**
   * Factory for creating instances.
   */
  public static class Factory implements ActionDefinitionFactory {
    @Override
    public ActionDefinition create(String body) {
      List<DataActionDefinition.LocationOperation> ops = DataActionUtils.deserializeBody(body);
      return new DataActionDefinition(ops);
    }

    @Override
    public String getName() {
      return NAME;
    }
  }

  /**
   * The list of LocationOperation defined for this action.
   * This is mainly used for serde.
   */
  private final List<LocationOperation> mDataOperations;
  private final Table<Location, Operation, Set<String>> mLocationModifiers =
      HashBasedTable.create();

  /**
   * @param dataOperations a list of (location, operation) pairs
   */
  public DataActionDefinition(List<LocationOperation> dataOperations) {
    mDataOperations = dataOperations;
    validate();
  }

  private void validate() {
    // Construct location modifiers table.
    for (LocationOperation op : mDataOperations) {
      Location l = op.getLocation();
      Operation o = op.getOperation();
      String lm = op.getLocationModifier();
      if (mLocationModifiers.contains(l, o) && mLocationModifiers.get(l, o).contains(lm)) {
        throw new IllegalStateException(String.format("Operation %s is duplicated", op));
      }
      if (mLocationModifiers.contains(l, o)) {
        mLocationModifiers.get(l, o).add(lm);
      } else {
        Set<String> s = new HashSet<>();
        s.add(lm);
        mLocationModifiers.put(l, o, s);
      }
    }

    // Validate constraints for UFS:STORE.
    if (mLocationModifiers.contains(Location.UFS, Operation.STORE)) {
      Set<String> locationModifiers = mLocationModifiers.get(Location.UFS, Operation.STORE);
      if (locationModifiers.size() > 1 && locationModifiers.contains("")) {
        throw new IllegalStateException("UFS:STOREs must all have location modifiers or not");
      }
    }

    // Validate constraints between UFS:REMOVE and UFS:STORE.
    if (mLocationModifiers.contains(Location.UFS, Operation.REMOVE)) {
      if (!mLocationModifiers.contains(Location.UFS, Operation.STORE)) {
        throw new IllegalStateException("UFS:REMOVE cannot exist without UFS:STORE");
      }
      if (mLocationModifiers.get(Location.UFS, Operation.REMOVE).contains("")) {
        throw new IllegalStateException("UFS:REMOVE must have location modifier");
      }
      if (mLocationModifiers.get(Location.UFS, Operation.STORE).contains("")) {
        throw new IllegalStateException("UFS:STORE must have location modifier when UFS:REMOVE "
            + "exists");
      }
      if (!Sets.intersection(mLocationModifiers.get(Location.UFS, Operation.REMOVE),
          mLocationModifiers.get(Location.UFS, Operation.STORE)).isEmpty()) {
        throw new IllegalStateException("UFS:REMOVE and UFS:STORE must not have shared location "
            + "modifier");
      }
    }

    // Remove empty location modifiers for UFS.
    if (mLocationModifiers.contains(Location.UFS, Operation.REMOVE)) {
      mLocationModifiers.get(Location.UFS, Operation.REMOVE).remove("");
    }
    if (mLocationModifiers.contains(Location.UFS, Operation.STORE)) {
      mLocationModifiers.get(Location.UFS, Operation.STORE).remove("");
    }
  }

  /**
   * @return whether ALLUXIO:STORE exists
   */
  public boolean hasAlluxioStore() {
    return mLocationModifiers.contains(Location.ALLUXIO, Operation.STORE);
  }

  /**
   * @return whether ALLUXIO:REMOVE exists
   */
  public boolean hasAlluxioRemove() {
    return mLocationModifiers.contains(Location.ALLUXIO, Operation.REMOVE);
  }

  /**
   * @return whether UFS:STORE exists
   */
  public boolean hasUfsStore() {
    return mLocationModifiers.contains(Location.UFS, Operation.STORE);
  }

  /**
   * @return whether UFS:REMOVE exists
   */
  public boolean hasUfsRemove() {
    return mLocationModifiers.contains(Location.UFS, Operation.REMOVE);
  }

  /**
   * @return the set of location modifiers for UFS:STORE, a location modifier will never be an
   *    empty string
   */
  public Set<String> getUfsStoreLocationModifiers() {
    return mLocationModifiers.get(Location.UFS, Operation.STORE);
  }

  /**
   * @return the set of location modifiers for UFS:REMOVE, a location modifier will never be an
   *    empty string
   */
  public Set<String> getUfsRemoveLocationModifiers() {
    return mLocationModifiers.get(Location.UFS, Operation.REMOVE);
  }

  @Override
  public ActionExecution createExecution(ActionExecutionContext ctx, String path,
      InodeState inodeState) {
    return new DataActionExecution(ctx, path, inodeState, this);
  }

  @Override
  public String serialize() {
    return String.format("%s(%s)", NAME, DataActionUtils.serializeBody(mDataOperations));
  }
}
