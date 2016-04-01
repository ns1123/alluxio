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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An {@link UnderFileSystem} using JDBC connections.
 */
@ThreadSafe
public class JDBCUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new instance of {@link JDBCUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for Alluxio
   */
  public JDBCUnderFileSystem(AlluxioURI uri, Configuration conf) {
    super(uri, conf);
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.JDBC;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void configureProperties() throws IOException {
    try {
      JDBCUtils.configureProperties(mUri, mProperties);
    } catch (IOException | SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    super.setProperties(properties);
    if (mProperties != null) {
      JDBCDriverRegistry.loadDriver(mProperties.get(UnderFileSystemConstants.JDBC_DRIVER_CLASS));
    }
  }

  @Override
  public void connectFromMaster(Configuration conf, String hostname) {
  }

  @Override
  public void connectFromWorker(Configuration conf, String hostname) {
  }

  @Override
  public OutputStream create(String path) throws IOException {
    throw new UnsupportedOperationException("JDBCUnderFileSystem does not support creating paths.");
  }

  @Override
  public OutputStream create(String path, int blockSizeByte) throws IOException {
    return create(path);
  }

  @Override
  public OutputStream create(String path, short replication, int blockSizeByte) throws IOException {
    return create(path);
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    throw new UnsupportedOperationException("JDBCUnderFileSystem does not deleting paths.");
  }

  @Override
  public boolean exists(String path) throws IOException {
    // Assuming the 'path' is the string representation of the full URI.

    AlluxioURI uri = new AlluxioURI(path);
    HashMap<String, String> properties = new HashMap<>(mProperties);
    properties.putAll(uri.getQueryMap());

    String table = properties.get(UnderFileSystemConstants.JDBC_TABLE);

    if (table == null || table.isEmpty()) {
      // no table.
      return false;
    }

    String filename = properties.get(UnderFileSystemConstants.JDBC_PARTITION_FILENAME);
    String numPartitionsStr = properties.get(UnderFileSystemConstants.JDBC_PARTITIONS);

    if (filename != null) {
      // Check for valid partition index.
      int partitionIndex = JDBCFilenameUtils.getPartitionFromFilename(filename);
      int numPartitions = Integer.parseInt(numPartitionsStr);
      if (partitionIndex < 0 || partitionIndex >= numPartitions) {
        // partition index is out-of-bounds.
        return false;
      }

      if (!filename.equals(JDBCFilenameUtils.getFilenameForPartition(partitionIndex, "csv"))) {
        // Incorrect filename.
        return false;
      }
    }
    return true;
  }

  /**
   * Gets the block size in bytes. Currently, the maximum allowed size of one file is 5 TB.
   *
   * @param path the file name
   * @return 5 TB in bytes
   * @throws IOException this implementation will not throw this exception, but subclasses may
   */
  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Constants.TB * 5;
  }

  // Not supported
  @Override
  public Object getConf() {
    LOG.warn("getConf is not supported when using JDBCUnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.warn("getFileLocations is not supported when using JDBCUnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    LOG.warn("getFileLocations is not supported when using JDBCUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    return Constants.UNKNOWN_SIZE;
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    return 0;
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    // Assuming the 'path' is the string representation of the full URI.

    AlluxioURI uri = new AlluxioURI(path);
    Map<String, String> queryMap = uri.getQueryMap();

    if (queryMap.get(UnderFileSystemConstants.JDBC_PARTITION_FILENAME) == null) {
      // There is no filename, so this URI is not a partition file.
      return false;
    }
    return true;
  }

  @Override
  public String[] list(String path) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    HashMap<String, String> properties = new HashMap<>(mProperties);
    properties.putAll(uri.getQueryMap());

    JDBCUtils.exists(path, properties.get(UnderFileSystemConstants.JDBC_USER),
        properties.get(UnderFileSystemConstants.JDBC_PASSWORD),
        properties.get(UnderFileSystemConstants.JDBC_TABLE),
        properties.get(UnderFileSystemConstants.JDBC_PARTITION_KEY));
    String[] files =
        new String[Integer.parseInt(properties.get(UnderFileSystemConstants.JDBC_PARTITIONS))];
    for (int i = 0; i < files.length; i++) {
      files[i] = JDBCFilenameUtils.getFilenameForPartition(i, "csv");
    }
    return files;
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    LOG.warn("mkdirs is not supported when using JDBCUnderFileSystem, returning false.");
    return false;
  }

  @Override
  public InputStream open(String path) throws IOException {
    // Assuming the 'path' is the string representation of the full URI.
    AlluxioURI uri = new AlluxioURI(path);
    HashMap<String, String> properties = new HashMap<>(mProperties);
    properties.putAll(uri.getQueryMap());

    String table = properties.get(UnderFileSystemConstants.JDBC_TABLE);
    String user = properties.get(UnderFileSystemConstants.JDBC_USER);
    String password = properties.get(UnderFileSystemConstants.JDBC_PASSWORD);
    String partitionKey = properties.get(UnderFileSystemConstants.JDBC_PARTITION_KEY);
    String projection = properties.get(UnderFileSystemConstants.JDBC_PROJECTION);
    String selection = properties.get(UnderFileSystemConstants.JDBC_WHERE);

    int partition = JDBCFilenameUtils
        .getPartitionFromFilename(properties.get(UnderFileSystemConstants.JDBC_PARTITION_FILENAME));
    if (partition == -1) {
      throw new IOException("Invalid partion for filename: " + properties
          .get(UnderFileSystemConstants.JDBC_PARTITION_FILENAME));
    }

    String partitionSelection =
        properties.get(UnderFileSystemConstants.JDBC_PROJECTION_CONDITION_PREFIX + partition);

    if (partitionSelection == null) {
      throw new IOException("No partition information for partition file: " + uri);
    }

    String fullSelection = partitionSelection;
    if (selection != null && !selection.isEmpty()) {
      // There is a general selection specified.
      if (fullSelection.isEmpty()) {
        // There is no specific partition selection (possible if there is only 1 partition)
        fullSelection = selection;
      } else {
        // There is a specific partition selection.
        fullSelection = "(" + selection + ") AND (" + partitionSelection + ")";
      }
    }

    return new JDBCInputStream(path, user, password, table, partitionKey, projection, fullSelection,
        properties);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    LOG.warn("rename is not supported when using JDBCUnderFileSystem, returning false.");
    return false;
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    HashMap<String, String> properties = new HashMap<>(mProperties);
    properties.putAll(ufsBaseUri.getQueryMap());

    String filename = alluxioPath;
    if (!filename.isEmpty() && filename.charAt(0) == '/') {
      // Strip away the starting slash
      filename = filename.substring(1);
    }
    properties.put(UnderFileSystemConstants.JDBC_PARTITION_FILENAME, filename);

    return new AlluxioURI(ufsBaseUri.getScheme(), ufsBaseUri.getAuthority(), ufsBaseUri.getPath(),
        properties);
  }

  @Override
  public void setConf(Object conf) {}

  @Override
  public void setPermission(String path, String posixPerm) throws IOException {}
}
