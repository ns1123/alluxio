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

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.UnderFileSystemConstants;
import alluxio.underfs.jdbc.meta.ColumnInfo;
import alluxio.underfs.jdbc.meta.PartitionInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for JDBC connections and values.
 */
public final class JDBCUtils {
  private JDBCUtils() {} // prevent instantiation

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
        case INTEGER: // intentionally fall through
        case LONG: // intentionally fall through
        case DOUBLE: // intentionally fall through
        case STRING: // intentionally fall through
        case TIME: // intentionally fall through
        case TIMESTAMP: // intentionally fall through
        case DECIMAL: // intentionally fall through
        case DATE: {
          Object val = resultSet.getObject(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return val.toString();
        }
        case BINARY: {
          // binary byte[] data cannot use toString() to convert to string.
          byte[] val = resultSet.getBytes(columnIndex);
          if (resultSet.wasNull()) {
            return "";
          }
          return new String(val);
        }
        default:
          throw new IOException(ExceptionMessage.SQL_UNSUPPORTED_COLUMN_TYPE
              .getMessage(columnInfo.getColumnType(), columnInfo.getName()));
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
    String user = properties.get(UnderFileSystemConstants.JDBC_USER);
    String password = properties.get(UnderFileSystemConstants.JDBC_PASSWORD);

    PartitionInfo partitionInfo =
        new PartitionInfo(ufsUri, user, password, tableName, partitionColumn, where);

    List<String> conditions = partitionInfo.getConditions(numPartitions);
    for (int i = 0; i < conditions.size(); i++) {
      properties
          .put(UnderFileSystemConstants.JDBC_PROJECTION_CONDITION_PREFIX + i, conditions.get(i));
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
   * Creates a query string with the given parameters.
   *
   * @param table the fully qualified table name
   * @param partitionKey the name of the partition column
   * @param projection the projection string for the query
   * @param selection the selection string for the query
   * @return the constructed query string
   */
  public static String constructQuery(String table, String partitionKey, String projection,
      String selection) {
    String query = "SELECT " + projection + " FROM " + table;
    if (selection != null && !selection.isEmpty()) {
      query += " WHERE " + selection;
    }
    query += " ORDER BY " + partitionKey + " ASC";
    return query;
  }
}
