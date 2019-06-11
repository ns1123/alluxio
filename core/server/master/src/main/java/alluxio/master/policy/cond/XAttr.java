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

import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;

import java.util.Map;

/**
 * A condition testing for the age of the path.
 */
public class XAttr implements Condition {
  private final String mKey;
  private final String mValue;

  /**
   * Factory for creating instances.
   */
  public static class Factory implements ConditionFactory {
    @Override
    public Condition deserialize(String serialized) {
      if (serialized.startsWith("xAttr(") && serialized.endsWith(")")) {
        serialized = serialized.substring("xAttr(".length(), serialized.length() - 1);
      }
      // split on the first comma
      String[] parts = serialized.split(",", 2);
      if (parts.length != 2) {
        return null;
      }
      // allowable values are null, and any quoted string.
      String value = parts[1].trim();
      if (value.toLowerCase().equals("null")) {
        return new XAttr(parts[0].trim(), null);
      }
      if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
        return new XAttr(parts[0].trim(), value.substring(1, value.length() - 1));
      }
      return null;
    }
  }

  /**
   * Creates an instance.
   *
   * @param key the xattr key
   * @param value the xattr value, can be null
   */
  public XAttr(String key, String value) {
    mKey = key;
    mValue = value;
  }

  @Override
  public IntervalSet evaluate(Interval interval, String path, InodeState state) {
    Map<String, byte[]> map = state.getXAttr();
    if (map == null) {
      return (mValue == null) ? new IntervalSet(interval) : IntervalSet.NEVER;
    }

    if (mValue == null) {
      return (map.containsKey(mKey)) ? IntervalSet.NEVER : new IntervalSet(interval);
    }

    byte[] value = map.get(mKey);
    if (value == null) {
      return IntervalSet.NEVER;
    }
    return (new String(value).equals(mValue)) ? new IntervalSet(interval) : IntervalSet.NEVER;
  }

  @Override
  public String serialize() {
    if (mValue == null) {
      return "xAttr(" + mKey + ",null)";
    }
    return "xAttr(" + mKey + ",\"" + mValue + "\")";
  }
}
