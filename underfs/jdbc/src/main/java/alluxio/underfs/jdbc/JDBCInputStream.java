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

import alluxio.underfs.jdbc.csv.CSVRowWriter;
import alluxio.underfs.jdbc.meta.ColumnInfo;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link InputStream} for a partition file of table, using a JDBC connection.
 */
@NotThreadSafe
public final class JDBCInputStream extends InputStream {
  // TODO(gpang): make these configurable parameters.
  private static final int INITIAL_BUFFER_BYTES = 1024;
  private static final int FETCH_SIZE = 100;

  private final String mConnectUri;
  private final String mUser;
  private final String mPassword;
  private final String mQuery;
  private final byte[] mSingleByte = new byte[1];

  /** The {@link ResultSet} storing the source data for this input stream. */
  private ResultSet mResultSet = null;
  /** The list of {@link ColumnInfo} for each column of the projection. */
  private ArrayList<ColumnInfo> mColumnList = null;
  /** A buffer to do the transformation from {@link #mResultSet} to bytes. */
  private ByteBuffer mBuffer = null;
  /** A handle to the previous row, if it was not written out to the buffer yet. Can be null. */
  private byte[] mPreviousRow = null;
  /** true if this stream is already closed. */
  private boolean mClosed;

  /**
   * Creates a {@link JDBCInputStream} instance.
   *
   * @param connectUri the JDBC connect string for UFS
   * @param user the user for the JDBC connection
   * @param password the password for the JDBC connection
   * @param table the fully qualified table name
   * @param partitionKey the name of the partition column
   * @param projection the projection string for the query
   * @param selection the selection string for the query
   * @param properties a {@link Map} of additional properties
   */
  public JDBCInputStream(String connectUri, String user, String password, String table,
      String partitionKey, String projection, String selection, Map<String, String> properties) {
    // TODO(gpang): the properties parameter is currently unused. Will support configuration later.
    Preconditions.checkNotNull(connectUri);
    mConnectUri = connectUri;
    mUser = user;
    mPassword = password;

    mClosed = false;
    mQuery = JDBCUtils.constructQuery(table, partitionKey, projection, selection);
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      if (mResultSet != null) {
        mResultSet.getStatement().getConnection().close();
      }
      mClosed = true;
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int read() throws IOException {
    if (read(mSingleByte, 0, 1) == -1) {
      return -1;
    }
    return (int) mSingleByte[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }

    checkResultSet();

    int readLen = 0;
    boolean done = false;

    while (!done && len > 0) {
      if (mBuffer.remaining() > 0) {
        // Copy already buffered data to destination.
        int copyLen = Math.min(len, mBuffer.remaining());
        mBuffer.get(b, off, copyLen);
        off += copyLen;
        len -= copyLen;
        readLen += copyLen;
      } else {
        // Buffer is empty, so fill it again.
        try {
          done = (fillBuffer() == 0);
        } catch (IOException | SQLException e) {
          throw new IOException(e);
        }
      }
    }
    if (done && readLen == 0) {
      return -1;
    }
    return readLen;
  }

  @Override
  public long skip(long n) throws IOException {
    checkResultSet();
    return super.skip(n);
  }

  /**
   * Checks to see if the {@link ResultSet} {@link #mResultSet} already exists, and creates it
   * otherwise.
   *
   * @throws IOException if there is an error creating the {@link ResultSet}
   */
  @SuppressFBWarnings({"SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", "ODR_OPEN_DATABASE_RESOURCE"})
  private void checkResultSet() throws IOException {
    if (mResultSet != null) {
      // Result set already exists.
      return;
    }

    Statement statement = null;
    boolean success = false;
    try {
      Connection connection = JDBCUtils.getConnection(mConnectUri, mUser, mPassword);
      statement =
          connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(FETCH_SIZE);
      mResultSet = statement.executeQuery(mQuery);
      ResultSetMetaData metadata = mResultSet.getMetaData();
      mColumnList = new ArrayList<>();
      for (int i = 0; i < metadata.getColumnCount(); i++) {
        // Columns are 1-indexed.
        mColumnList.add(
            new ColumnInfo(metadata.getColumnType(i + 1), metadata.getColumnClassName(i + 1),
                metadata.getColumnTypeName(i + 1), metadata.getColumnLabel(i + 1)));
      }
      success = true;
    } catch (SQLException e) {
      throw new IOException("Error in query: " + mQuery + " error: " + e.getMessage(), e);
    } finally {
      if (!success && statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          throw new IOException(e);
        }
      }
    }
    allocateBuffer(INITIAL_BUFFER_BYTES);
    prepareBufferForRead();
  }

  /**
   * Allocates a new internal buffer of a specified size.
   *
   * @param size the new size for the buffer
   */
  private void allocateBuffer(int size) {
    mBuffer = ByteBuffer.allocate(size);
    mBuffer.clear();
  }

  /**
   * Prepares the internal buffer for reading.
   */
  private void prepareBufferForRead() {
    mBuffer.flip();
  }

  /**
   * Fills up the buffer from the {@link #mResultSet} results. This assumes that the existing
   * buffer has already be fully consumed.
   *
   * @return the number of bytes written to the buffer
   * @throws SQLException if there is an error retrieving values from the {@link ResultSet}
   * @throws IOException if there is an error writing to the buffer
   */
  private int fillBuffer() throws SQLException, IOException {
    // Assume buffer is already fully consumed.
    Preconditions.checkNotNull(mBuffer);
    Preconditions.checkState(mBuffer.remaining() == 0);
    // Clear the buffer before writing into it.
    mBuffer.clear();

    // Write transformed data into the buffer.
    while (mPreviousRow != null || mResultSet.next()) {
      byte[] bytes = mPreviousRow;
      if (bytes == null) {
        // There is no data for the previous row. Instead, transform the column data from the
        // result set to a byte[].
        RowWriter rowWriter = new CSVRowWriter();
        for (int i = 0; i < mColumnList.size(); i++) {
          String value = JDBCUtils.getStringValue(mColumnList.get(i), mResultSet, i + 1);
          rowWriter.writeValue(value);
        }
        rowWriter.close();
        bytes = rowWriter.getBytes();
      }

      if (bytes.length > mBuffer.remaining()) {
        // Not enough space in the buffer.
        if (mBuffer.position() == 0) {
          // No data in the buffer, so create a new buffer with a bigger size.
          allocateBuffer(bytes.length);
        } else {
          // Current row does not fit, but there is some data already in the buffer.
          mPreviousRow = bytes;
          break;
        }
      }

      // Put the row data into the buffer.
      mBuffer.put(bytes);
      mPreviousRow = null;
    }

    // The buffer is filled with transformed data. Prepare it for reading.
    prepareBufferForRead();
    return mBuffer.limit();
  }
}
