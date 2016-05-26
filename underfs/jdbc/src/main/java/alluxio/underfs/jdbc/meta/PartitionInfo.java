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

package alluxio.underfs.jdbc.meta;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.jdbc.JDBCUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * This class stores and calculates information about a partition column of a table.
 */
public final class PartitionInfo {
  private final AlluxioURI mUri;
  private final String mUser;
  private final String mPassword;
  private final String mTable;
  private final String mPartitionColumn;
  private final String mSelection;

  /**
   * Creates a {@link PartitionInfo} instance.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param user the user for the JDBC connection
   * @param password the password for the JDBC connection
   * @param table the fully qualified table name
   * @param partitionColumn the name of the partition column
   * @param selection the selection string for the query
   */
  public PartitionInfo(AlluxioURI uri, String user, String password, String table,
      String partitionColumn, String selection) {
    mUri = uri;
    mUser = user;
    mPassword = password;
    mTable = table;
    mPartitionColumn = partitionColumn;
    mSelection = selection;
  }

  /**
   * @param numPartitions the number of desired partitions
   * @return a list of partition predicates, one entry for each partition
   * @throws IOException if the partition column type is unsupported
   * @throws SQLException if there is an error in retrieving the values from the result set
   */
  @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
  public List<String> getConditions(int numPartitions) throws IOException, SQLException {
    // The list of condition strings to return.
    List<String> conditions = new ArrayList<>(numPartitions);

    // Run a query to get the min and max of the partition column.
    String projection = "MIN(" + mPartitionColumn + "), MAX(" + mPartitionColumn + ")";
    int columnCount = 2;
    String query = "SELECT " + projection + " FROM " + mTable;
    if (mSelection != null && !mSelection.isEmpty()) {
      query += " WHERE " + mSelection;
    }

    try (
        Connection connection = JDBCUtils.getConnection(mUri.toString(), mUser, mPassword);
        PreparedStatement statement = connection.prepareStatement(query);
        ResultSet resultSet = statement.executeQuery()) {

      resultSet.next();

      ResultSetMetaData metadata = resultSet.getMetaData();
      int columns = metadata.getColumnCount();
      if (columns != columnCount) {
        throw new IOException(
            ExceptionMessage.SQL_UNEXPECTED_COLUMN_COUNT.getMessage(columnCount, columns));
      }
      // Get column info for first column.
      ColumnInfo columnInfo =
          new ColumnInfo(metadata.getColumnType(1), metadata.getColumnClassName(1),
              metadata.getColumnTypeName(1), metadata.getColumnName(1));
      // Make sure other column is of same type.
      int secondType = metadata.getColumnType(2);
      if (columnInfo.getSqlType() != secondType || !columnInfo.getDbTypeName()
          .equals(metadata.getColumnTypeName(2))) {
        throw new IOException(ExceptionMessage.SQL_UNEXPECTED_COLUMN_TYPE
            .getMessage(columnInfo.getName(), columnInfo.getSqlType(), secondType));
      }

      // Get the partition boundaries.
      List<String> lowerBoundaries = getPartitionBoundaries(columnInfo, resultSet, numPartitions);
      for (int i = 0; i < numPartitions; i++) {
        String lower = lowerBoundaries.get(i);
        String upper = null;
        if (i < numPartitions - 1) {
          upper = lowerBoundaries.get(i + 1);
        }
        if (i == 0) {
          lower = null;
        }
        // The condition for the individual partition.
        String condition = "";
        if (lower != null) {
          condition += mPartitionColumn + " >= " + lower;
        }
        if (upper != null) {
          if (!condition.isEmpty()) {
            condition += " AND ";
          }
          condition += mPartitionColumn + " < " + upper;
        }
        conditions.add(condition);
      }
    }
    return conditions;
  }

  /**
   * Returns a list of string values, representing the lower boundary values for each partition.
   *
   * @param columnInfo the {@link ColumnInfo} for the column
   * @param resultSet the {@link ResultSet} holding the min and max for the partition column
   * @param numPartitions the number of desired partitions
   * @return a list of string values for partition lower boundaries
   * @throws IOException
   * @throws SQLException
   */
  private List<String> getPartitionBoundaries(ColumnInfo columnInfo, ResultSet resultSet,
      int numPartitions) throws IOException, SQLException {
    List<String> lowerBoundaries = new ArrayList<>(numPartitions);
    // TODO(gpang): add support for more types.
    switch (columnInfo.getColumnType()) {
      case INTEGER: {
        int lower = resultSet.getInt(1);
        int upper = resultSet.getInt(2);
        int size = (upper - lower) / numPartitions;
        int boundary = lower;
        for (int i = 0; i < numPartitions; i++) {
          lowerBoundaries.add(Integer.toString(boundary));
          boundary += size;
        }
        break;
      }
      case LONG: {
        long lower = resultSet.getLong(1);
        long upper = resultSet.getLong(2);
        long size = (upper - lower) / numPartitions;
        long boundary = lower;
        for (int i = 0; i < numPartitions; i++) {
          lowerBoundaries.add(Long.toString(boundary));
          boundary += size;
        }
        break;
      }
      case DOUBLE: {
        double lower = resultSet.getDouble(1);
        double upper = resultSet.getDouble(2);
        double size = (upper - lower) / numPartitions;
        double boundary = lower;
        for (int i = 0; i < numPartitions; i++) {
          lowerBoundaries.add(Double.toString(boundary));
          boundary += size;
        }
        break;
      }
      case DECIMAL: {
        BigDecimal lower = resultSet.getBigDecimal(1);
        BigDecimal upper = resultSet.getBigDecimal(2);
        BigDecimal size = upper.subtract(lower).divide(BigDecimal.valueOf(numPartitions));
        BigDecimal boundary = lower;
        for (int i = 0; i < numPartitions; i++) {
          lowerBoundaries.add(boundary.toString());
          boundary = boundary.add(size);
        }
        break;
      }
      case DATE: {
        Date lower = resultSet.getDate(1);
        Date upper = resultSet.getDate(2);
        long size = (upper.getTime() - lower.getTime()) / numPartitions;
        long boundary = lower.getTime();
        for (int i = 0; i < numPartitions; i++) {
          lower.setTime(boundary);
          lowerBoundaries.add("\"" + lower.toString() + "\"");
          boundary += size;
        }
        break;
      }
      case TIMESTAMP: {
        Timestamp lower = resultSet.getTimestamp(1);
        Timestamp upper = resultSet.getTimestamp(2);
        long size = (upper.getTime() - lower.getTime()) / numPartitions;
        long boundary = lower.getTime();
        for (int i = 0; i < numPartitions; i++) {
          lower.setTime(boundary);
          lowerBoundaries.add("\"" + lower.toString() + "\"");
          boundary += size;
        }
        break;
      }
      default:
        throw new IOException(ExceptionMessage.SQL_UNSUPPORTED_PARTITION_COLUMN_TYPE
            .getMessage(columnInfo.getColumnType(), columnInfo.getDbTypeName()));
    }
    // Iterate over all the boundary values and make sure there are no duplicates.
    // A duplicate means the range is not large enough to support the number of partitions.
    // TODO(gpang): support situation when range does not support specified number of partitions.
    for (int i = 1; i < lowerBoundaries.size(); i++) {
      if (lowerBoundaries.get(i).equals(lowerBoundaries.get(i - 1))) {
        throw new IOException(ExceptionMessage.SQL_NUM_PARTITIONS_TOO_LARGE
            .getMessage(mPartitionColumn, numPartitions));
      }
    }
    return lowerBoundaries;
  }
}
