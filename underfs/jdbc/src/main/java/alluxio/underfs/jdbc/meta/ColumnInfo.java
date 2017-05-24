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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Information about a column.
 */
@ThreadSafe
public final class ColumnInfo {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnInfo.class);

  /** The {@link java.sql.Types} type of the column. */
  private final int mSqlType;
  /** The fully-qualified name of the Java class for the column. */
  private final String mClassName;
  /** The name of the database-specific type name for the column. */
  private final String mDbTypeName;
  /** The name of the column. */
  private final String mName;
  /** The {@link ColumnType} of the column. */
  private final ColumnType mType;

  /**
   * Creates an instance.
   *
   * @param sqlType the {@link java.sql.Types} type of the column
   * @param className the fully-qualified name of the Java class for the column
   * @param dbTypeName the name of the database-specific type name for the column
   * @param name the name of the column
   */
  public ColumnInfo(int sqlType, String className, String dbTypeName, String name) {
    mSqlType = sqlType;
    mClassName = className;
    mDbTypeName = dbTypeName;
    mName = name;
    mType = determineColumnType(mSqlType);
  }

  /**
   * @return the {@link java.sql.Types} type of the column
   */
  public int getSqlType() {
    return mSqlType;
  }

  /**
   * @return the fully-qualified name of the Java class for the column
   */
  public String getClassName() {
    return mClassName;
  }

  /**
   * @return the name of the database-specific type name for the column
   */
  public String getDbTypeName() {
    return mDbTypeName;
  }

  /**
   * @return the name of the column
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the {@link ColumnType} of the column
   */
  public ColumnType getColumnType() {
    return mType;
  }

  /**
   * @param sqlType the input {@link java.sql.Types} type
   * @return the {@link ColumnType} of the column
   */
  private static ColumnType determineColumnType(int sqlType) {
    // TODO(gpang): add support for more types and different database systems.
    // TODO(gpang): incorporate db type information for this logic.
    switch (sqlType) {
      case Types.BOOLEAN: return ColumnType.INTEGER;
      case Types.BIT: return ColumnType.INTEGER;
      case Types.TINYINT: return ColumnType.INTEGER;
      case Types.SMALLINT: return ColumnType.INTEGER;
      case Types.INTEGER: return ColumnType.INTEGER;
      case Types.BIGINT: return ColumnType.LONG;
      case Types.FLOAT: return ColumnType.FLOAT;
      case Types.REAL: return ColumnType.DOUBLE;
      case Types.DOUBLE: return ColumnType.DOUBLE;
      case Types.NCLOB: return ColumnType.STRING;
      case Types.CLOB: return ColumnType.STRING;
      case Types.NCHAR: return ColumnType.STRING;
      case Types.NVARCHAR: return ColumnType.STRING;
      case Types.LONGNVARCHAR: return ColumnType.STRING;
      case Types.CHAR: return ColumnType.STRING;
      case Types.VARCHAR: return ColumnType.STRING;
      case Types.LONGVARCHAR: return ColumnType.STRING;
      case Types.TIME: return ColumnType.TIME;
      case Types.TIMESTAMP: return ColumnType.TIMESTAMP;
      case Types.BLOB: return ColumnType.BINARY;
      case Types.BINARY: return ColumnType.BINARY;
      case Types.VARBINARY: return ColumnType.BINARY;
      case Types.LONGVARBINARY: return ColumnType.BINARY;
      case Types.NUMERIC: return ColumnType.DECIMAL;
      case Types.DECIMAL: return ColumnType.DECIMAL;
      case Types.DATE: return ColumnType.DATE;
      case Types.NULL: // intentionally fall through
      case Types.OTHER: // intentionally fall through
      case Types.JAVA_OBJECT: // intentionally fall through
      case Types.DISTINCT: // intentionally fall through
      case Types.STRUCT: // intentionally fall through
      case Types.ARRAY: // intentionally fall through
      case Types.REF: // intentionally fall through
      case Types.DATALINK: // intentionally fall through
      case Types.ROWID: // intentionally fall through
      case Types.SQLXML: // intentionally fall through
      default:
        LOG.warn("Unsupported sql type: " + sqlType);
        return ColumnType.UNSUPPORTED;
    }
  }
}
