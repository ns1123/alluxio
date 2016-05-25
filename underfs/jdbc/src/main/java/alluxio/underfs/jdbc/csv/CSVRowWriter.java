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

package alluxio.underfs.jdbc.csv;

import alluxio.underfs.jdbc.RowWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link RowWriter} that writes the values as CSV, according to RFC 4180.
 */
@NotThreadSafe
public final class CSVRowWriter implements RowWriter {
  private static final int INITIAL_STREAM_SIZE = 1024;
  private static final char FIELD_SEPARATOR = ',';
  private static final char ENCLOSING = '"';
  private static final char CARRIAGE_RETURN = '\r';
  private static final char NEW_LINE = '\n';

  private final ByteArrayOutputStream mStream;
  private final OutputStreamWriter mWriter;

  private byte[] mBytes = null;
  private boolean mClosed = false;
  private boolean mAddSeparator = false;

  /**
   * Constructs a {@link CSVRowWriter}.
   */
  public CSVRowWriter() {
    mStream = new ByteArrayOutputStream(INITIAL_STREAM_SIZE);
    mWriter = new OutputStreamWriter(mStream);
  }

  @Override
  public void writeValue(Object value) throws IOException {
    if (mClosed) {
      throw new IOException("Cannot write to a closed row.");
    }
    String str = value.toString();
    boolean enclose = hasSpecialCharacters(str);
    if (mAddSeparator) {
      mWriter.write(FIELD_SEPARATOR);
    }
    if (enclose) {
      // If the string has any special characters, it must be enclosed by double quotes.
      mWriter.write(ENCLOSING);
      writeEnclosed(str);
      mWriter.write(ENCLOSING);
    } else {
      mWriter.write(str, 0, str.length());
    }
    mAddSeparator = true;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mWriter.write(NEW_LINE);
    mWriter.close();
    mBytes = mStream.toByteArray();
    mClosed = true;
  }

  @Override
  public byte[] getBytes() {
    return mBytes;
  }

  /**
   * @param str the input string
   * @return true if the input has special CSV characters, false otherwise
   */
  private boolean hasSpecialCharacters(String str) {
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == FIELD_SEPARATOR || c == ENCLOSING || c == NEW_LINE || c == CARRIAGE_RETURN) {
        return true;
      }
    }
    return false;
  }

  /**
   * Writes a string to {@link #mWriter} for an enclosed string. This means the string may have
   * to escape special CSV characters.
   *
   * @param str the string to write
   * @throws IOException if an error occurs
   */
  private void writeEnclosed(String str) throws IOException {
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == ENCLOSING) {
        // RFC-4180: double quote is escaped by preceding with another double quote. Other
        // special characters do not have to be escaped.
        mWriter.write(ENCLOSING);
      }
      mWriter.write(c);
    }
  }
}
