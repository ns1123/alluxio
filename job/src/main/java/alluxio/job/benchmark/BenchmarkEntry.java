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
    private final String tableName;
    @JsonProperty(value = "column_names")
    private final List<String> columnNames;
    @JsonProperty(value = "column_types")
    private final List<String> columnTypes;
    @JsonProperty(value = "results")
    private final Map<String, Object> results;

    public BenchmarkEntry(String tableName, List<String> columnNames, List<String> columnTypes, Map<String, Object> results) {
        this.tableName = tableName;
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        this.results = ImmutableMap.copyOf(results);
        Preconditions.checkArgument(columnNames.size() == columnTypes.size(),
                "Number of column names (%s) and types (%s) should be equal but were %s and %s",
                columnNames.size(), columnTypes.size(), columnNames, columnTypes);
        Preconditions.checkArgument(columnNames.size() == results.size(),
                "Number of columns %s and results %s should be equal but results were %s",
                columnNames.size(), results.size(), results);
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public Map<String, Object> getResults() {
        return results;
    }

    @JsonIgnore
    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
