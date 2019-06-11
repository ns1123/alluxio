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

import alluxio.master.file.meta.PersistenceState;

import com.google.common.base.Preconditions;

/**
 * An implementation of an extended attribute for {@link PersistenceState}.
 */
public class PersistenceStateAttribute extends AbstractExtendedAttribute<PersistenceState> {

  /** Minimum number of bytes needed to store a persistence state. */
  private static final int ENCODING_SIZE =
      (int) Math.ceil(Math.log((double) PersistenceState.values().length) / Math.log(2.0) / 8.0);

  PersistenceStateAttribute() {
    super(NamespacePrefix.SYSTEM, "ps");
  }

  @Override
  public byte[] encode(PersistenceState state) {
    byte[] buff = new byte[ENCODING_SIZE];
    buff[0] = (byte) state.ordinal();
    return buff;
  }

  @Override
  public PersistenceState decode(byte[] bytes) {
    Preconditions.checkArgument(bytes.length == ENCODING_SIZE, String.format("Cannot decode "
        + "persistence state attribute. Byte array is not a multiple of encoding size. Got %d, "
        + "must be a multiple of %d.", bytes.length, ENCODING_SIZE));

    int loc = bytes[0] & 0xFF;
    return PersistenceState.values()[loc];
  }

  /**
   * Get the full name of the attribute for the persistence state with a given ID.
   *
   * @param id the id to concat onto the attribute name
   * @return the concatenated id with this attribute. i.e. {@code system.ps.{id}}
   */
  public String forId(String id) {
    Preconditions.checkState(id.length() > 0, "id for persistence state attribute must not be "
        + "empty string");
    return String.join(".", getName(), id);
  }

  /**
   * Get the ID from a persistence attribute name.
   *
   * @param fullAttr the full attribute name. i.e. {@code system.ps.{ID}}
   * @return {@code ID} from the full attribute name
   */
  public String fromName(String fullAttr) {
    Preconditions.checkState(fullAttr.length() > getName().length());
    return fullAttr.substring(getName().length());
  }
}
