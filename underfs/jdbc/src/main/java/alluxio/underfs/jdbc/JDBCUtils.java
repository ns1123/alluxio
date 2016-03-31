/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.jdbc;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystemConstants;
import alluxio.underfs.jdbc.meta.ColumnInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for JDBC connections and values.
 */
public class JDBCUtils {

  /**
   * Returns the string representation of the value, specified by an index into a {@link ResultSet}.
   *
   * @param columnInfo the {@link ColumnInfo} for the column
   * @param resultSet the {@link ResultSet} to get the value from
   * @param columnIndex the (1-indexed) index into the given {@link ResultSet} to get the value from
   * @return the string representation of the value
   * @throws IOException if there is an error retrieving the value
   */
  public static String getStringValue(ColumnInfo columnInfo, ResultSet resultSet, int columnIndex)
      throws IOException {
    try {
      switch (columnInfo.getColumnType()) {
        case INTEGER: {
          int val = resultSet.getInt(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return Integer.toString(val);
        }
        case LONG: {
          long val = resultSet.getLong(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return Long.toString(val);
        }
        case DOUBLE: {
          double val = resultSet.getDouble(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return Double.toString(val);
        }
        case STRING: {
          String val = resultSet.getString(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return val;
        }
        case TIME: {
          Time val = resultSet.getTime(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return val.toString();
        }
        case TIMESTAMP: {
          Timestamp val = resultSet.getTimestamp(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return val.toString();
        }
        case DECIMAL: {
          BigDecimal val = resultSet.getBigDecimal(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return val.toString();
        }
        case DATE: {
          Date val = resultSet.getDate(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return val.toString();
        }
        case BINARY: {
          byte[] val = resultSet.getBytes(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return new String(val);
        }
        default:
          throw new IOException("Unsupported column type: " + columnInfo.getColumnType());
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  /**
   * @param ufsUri the string representation of the uri
   * @param user the user for the JDBC connection
   * @param password the password for the JDBC connection
   * @param tableName fully qualified table name
   * @param partitionColumn the name of the partition column
   * @return true if the table exists, false otherwise
   */
  @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
  public static boolean exists(String ufsUri, String user, String password, String tableName,
      String partitionColumn) {
    try (
        Connection connection = getConnection(ufsUri, user, password);
        PreparedStatement statement = connection.prepareStatement(
            "SELECT " + partitionColumn + " FROM " + tableName + " WHERE 1=0")) {
      ResultSet resultSet = statement.executeQuery();
      resultSet.close();
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Configures and updates the properties for the JDBC connection. This primarily determines the
   * partitioning conditions. This will modify the input map.
   *
   * @param ufsUri the {@link AlluxioURI} for this UFS
   * @param properties the properties map. This method will update this map
   * @throws IOException if there is an error in configuring the properties
   * @throws SQLException if there is an error in accessing the table
   */
  @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
  public static void configureProperties(AlluxioURI ufsUri, Map<String, String> properties)
      throws IOException, SQLException {
    String tableName = properties.get(UnderFileSystemConstants.JDBC_TABLE);
    int numPartitions = Integer.parseInt(properties.get(UnderFileSystemConstants.JDBC_PARTITIONS));
    String partitionColumn = properties.get(UnderFileSystemConstants.JDBC_PARTITION_KEY);
    String where = properties.get(UnderFileSystemConstants.JDBC_WHERE);

    // Run a query to get the min and max of the partition column.
    String selection = "MIN(" + partitionColumn + "), MAX(" + partitionColumn + ")";
    int selectionCount = 2;
    String query = "SELECT " + selection + " FROM " + tableName;
    if (where != null && !where.isEmpty()) {
      query += " WHERE " + where;
    }

    try (
        Connection connection = getConnection(ufsUri.toString(),
            properties.get(UnderFileSystemConstants.JDBC_USER),
            properties.get(UnderFileSystemConstants.JDBC_PASSWORD));
        PreparedStatement statement = connection.prepareStatement(query)) {
      statement.setFetchSize(10);
      ResultSet resultSet = statement.executeQuery();
      resultSet.next();

      ResultSetMetaData metadata = resultSet.getMetaData();
      int columns = metadata.getColumnCount();
      if (columns != selectionCount) {
        throw new SQLException("Unexpected number of columns while determining partition bounds.");
      }
      // Get column info for first column.
      ColumnInfo columnInfo =
          new ColumnInfo(metadata.getColumnType(1), metadata.getColumnClassName(1),
              metadata.getColumnTypeName(1), metadata.getColumnTypeName(1));
      // Make sure other column is of same type.
      if (columnInfo.getSqlType() != metadata.getColumnType(2) || !columnInfo.getName()
          .equals(metadata.getColumnTypeName(2))) {
        throw new SQLException(
            "Unexpected column type mismatch while determining partition bounds.");
      }

      List<String> conditions =
          getPartitionConditions(columnInfo, resultSet, partitionColumn, numPartitions);
      for (int i = 0; i < conditions.size(); i++) {
        properties
            .put(UnderFileSystemConstants.JDBC_PROJECTION_CONDITION_PREFIX + i, conditions.get(i));
      }
    } catch (SQLException e) {
      throw e;
    }
  }

  /**
   * Connects and returns the JDBC {@link Connection} using the given user and password. If
   * either are null, they will not be used to connect. Instead, the credentials are assumed to
   * already specified within the URI.
   *
   * @param uri the JDBC URI
   * @param user the user for the JDBC connection
   * @param password the password for the JDBC connection
   * @return the {@link Connection} for the given uri and credentials
   * @throws SQLException if there is an error creating a connection
   */
  public static Connection getConnection(String uri, String user, String password)
      throws SQLException {
    if (user != null && password != null) {
      return DriverManager.getConnection(uri, user, password);
    }
    return DriverManager.getConnection(uri);
  }

  /**
   * @param columnInfo the {@link ColumnInfo} for the column
   * @param resultSet the {@link ResultSet} holding the min and max for the partition column
   * @param partitionColumn the name of the partition column
   * @param numPartitions the number of desired partitions
   * @return a list of partition predicates, one entry for each partition
   * @throws IOException if the partition column type is unsupported
   * @throws SQLException if there is an error in retrieving the values from the result set
   */
  private static List<String> getPartitionConditions(ColumnInfo columnInfo, ResultSet resultSet,
      String partitionColumn, int numPartitions) throws IOException, SQLException {
    List<String> lowerBoundaries = getPartitionBoundaries(columnInfo, resultSet, numPartitions);
    List<String> conditions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      String lower = lowerBoundaries.get(i);
      String upper = null;
      if (i < numPartitions - 1) {
        upper = lowerBoundaries.get(i + 1);
      }
      if (i == 0) {
        lower = null;
      }
      String condition = "";
      if (lower != null) {
        condition += partitionColumn + " >= " + lower;
      }
      if (upper != null) {
        if (!condition.isEmpty()) {
          condition += " AND ";
        }
        condition += partitionColumn + " < " + upper;
      }
      conditions.add(condition);
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
  private static List<String> getPartitionBoundaries(ColumnInfo columnInfo, ResultSet resultSet,
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
      case TIMESTAMP:
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
      default:
        throw new IOException("Unsupported partition column type: " + columnInfo.getName());
    }
    return lowerBoundaries;
  }
}
