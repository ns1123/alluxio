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

package alluxio.underfs.jdbc;

import java.io.IOException;

/**
 * A {@link RowWriter} writes values to a single row, and converts the row into a byte array.
 */
// TODO(gpang): Rethink this interface once there are writers other than CSV. Future APIs:
// - write() will need column information
// - future writers will probably need capability for writing header and/or footer
public interface RowWriter {
  /**
   * Writes a value to the row.
   *
   * @param value the value to write
   * @throws IOException if an error occurs
   */
  void writeValue(Object value) throws IOException;

  /**
   * Closes the row. After closing a row, additional values cannot be written.
   *
   * @throws IOException if an error occurs
   */
  void close() throws IOException;

  /**
   * Returns a byte array representation of the row. This must be called after {@link #close()}.
   *
   * @return a byte array representation of the row
   */
  byte[] getBytes();
}
