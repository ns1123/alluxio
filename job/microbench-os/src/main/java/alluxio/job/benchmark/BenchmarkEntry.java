/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * A benchmark entry deserialized by the microbench callback in autobots.
 * This needs to stay consistent with BenchmarkEntry in results_table.go.
 */
public final class BenchmarkEntry {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @JsonProperty(value = "table_name")
  private final String mTableName;
  @JsonProperty(value = "column_names")
  private final List<String> mColumnNames;
  @JsonProperty(value = "column_types")
  private final List<String> mColumnTypes;
  @JsonProperty(value = "results")
  private final Map<String, Object> mResults;

  /**
   * Constructors a new {@link BenchmarkEntry}.
   *
   * @param tableName the tale name
   * @param columnNames the column names
   * @param columnTypes the column types
   * @param results the results
   */
  public BenchmarkEntry(String tableName, List<String> columnNames, List<String> columnTypes,
      Map<String, Object> results) {
    mTableName = tableName;
    mColumnNames = ImmutableList.copyOf(columnNames);
    mColumnTypes = ImmutableList.copyOf(columnTypes);
    mResults = ImmutableMap.copyOf(results);
    Preconditions.checkArgument(columnNames.size() == columnTypes.size(),
        "Number of column names (%s) and types (%s) should be equal but were %s and %s",
        columnNames.size(), columnTypes.size(), columnNames, columnTypes);
    Preconditions.checkArgument(columnNames.size() == results.size(),
        "Number of columns %s and results %s should be equal but results were %s",
        columnNames.size(), results.size(), results);
  }

  /**
   * @return the table name
   */
  public String getTableName() {
    return mTableName;
  }

  /**
   * @return the column names
   */
  public List<String> getColumnNames() {
    return mColumnNames;
  }

  /**
   * @return the column types
   */
  public List<String> getColumnTypes() {
    return mColumnTypes;
  }

  /**
   * @return the results
   */
  public Map<String, Object> getResults() {
    return mResults;
  }

  /**
   * Converts to json format.
   *
   * @return the json format
   */
  @JsonIgnore
  public String toJson() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
