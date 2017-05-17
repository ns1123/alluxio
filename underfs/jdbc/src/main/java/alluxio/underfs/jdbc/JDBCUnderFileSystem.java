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
import alluxio.Constants;
import alluxio.underfs.BaseUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemConstants;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

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
public final class JDBCUnderFileSystem extends BaseUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCUnderFileSystem.class);
  // TODO(gpang): support more formats/extensions and make this a configurable parameter.
  private static final String FILE_EXTENSION = "csv";

  /**
   * Constructs a new instance of {@link JDBCUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   */
  public JDBCUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
    try {
      // TODO(gene): validate that this works
      JDBCUtils.configureProperties(mUri, mUfsConf.getUserSpecifiedConf());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getUnderFSType() {
    return "jdbc";
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void connectFromMaster(String hostname) {
  }

  @Override
  public void connectFromWorker(String hostname) {
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    throw new UnsupportedOperationException("JDBCUnderFileSystem does not support creating paths.");
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    throw new UnsupportedOperationException("JDBCUnderFileSystem does not deleting paths.");
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    throw new UnsupportedOperationException("JDBCUnderFileSystem does not deleting paths.");
  }

  @Override
  public boolean exists(String path) throws IOException {
    // Assuming the 'path' is the string representation of the full URI.

    AlluxioURI uri = new AlluxioURI(path);
    HashMap<String, String> properties = new HashMap<>(mUfsConf.getUserSpecifiedConf());
    properties.putAll(uri.getQueryMap());

    String table = properties.get(UnderFileSystemConstants.JDBC_TABLE);

    if (table == null || table.isEmpty()) {
      // The table property is not set in the URI.
      return false;
    }

    String filename = properties.get(UnderFileSystemConstants.JDBC_PARTITION_FILENAME);
    String numPartitionsStr = properties.get(UnderFileSystemConstants.JDBC_PARTITIONS);

    if (filename != null) {
      // Check for valid partition index.
      int partitionIndex = JDBCFilenameUtils.getPartitionFromFilename(filename);
      int numPartitions;
      try {
        numPartitions = Integer.parseInt(numPartitionsStr);
      } catch (NumberFormatException e) {
        return false;
      }
      if (partitionIndex < 0 || partitionIndex >= numPartitions) {
        // partition index is out-of-bounds.
        return false;
      }

      if (!filename
          .equals(JDBCFilenameUtils.getFilenameForPartition(partitionIndex, FILE_EXTENSION))) {
        // Incorrect filename.
        return false;
      }
    }
    // filename is not set in the URI. Therefore, just test the connection.
    try {
      JDBCUtils.getConnection(path, properties.get(UnderFileSystemConstants.JDBC_USER),
          properties.get(UnderFileSystemConstants.JDBC_PASSWORD)).close();
    } catch (SQLException e) {
      return false;
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

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    LOG.debug("getDirectoryStatus not supported in JDBCUnderFileSystem");
    return new UfsDirectoryStatus(path, null, null, Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.warn("getFileLocations is not supported when using JDBCUnderFileSystem, returning null.");
    return null;
  }

  // Not supported
  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options) throws IOException {
    LOG.warn("getFileLocations is not supported when using JDBCUnderFileSystem, returning null.");
    return null;
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    LOG.debug("getFileStatus not supported in JDBCUnderFileSystem");
    return new UfsFileStatus(path, Constants.UNKNOWN_SIZE, -1, null, null,
        Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  // This call is currently only used for the web ui, where a negative value implies unknown.
  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return false;
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
  public UfsStatus[] listStatus(String path) throws IOException {
    AlluxioURI uri = new AlluxioURI(path);
    HashMap<String, String> properties = new HashMap<>(mUfsConf.getUserSpecifiedConf());
    properties.putAll(uri.getQueryMap());

    JDBCUtils.exists(path, properties.get(UnderFileSystemConstants.JDBC_USER),
        properties.get(UnderFileSystemConstants.JDBC_PASSWORD),
        properties.get(UnderFileSystemConstants.JDBC_TABLE),
        properties.get(UnderFileSystemConstants.JDBC_PARTITION_KEY));
    UfsStatus[] files =
        new UfsStatus[Integer.parseInt(properties.get(UnderFileSystemConstants.JDBC_PARTITIONS))];
    for (int i = 0; i < files.length; i++) {
      String name = JDBCFilenameUtils.getFilenameForPartition(i, FILE_EXTENSION);
      files[i] = new UfsFileStatus(name, Constants.UNKNOWN_SIZE, -1, null, null,
          Constants.DEFAULT_FILE_SYSTEM_MODE);
    }
    return files;
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    LOG.warn("mkdirs is not supported when using JDBCUnderFileSystem, returning false.");
    return false;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    // Assuming the 'path' is the string representation of the full URI.
    AlluxioURI uri = new AlluxioURI(path);
    HashMap<String, String> properties = new HashMap<>(mUfsConf.getUserSpecifiedConf());
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

    InputStream inputStream = new JDBCInputStream(path, user, password, table, partitionKey,
        projection, fullSelection, properties);
    if (options.getOffset() > 0) {
      try {
        inputStream.skip(options.getOffset());
      } catch (IOException e) {
        inputStream.close();
        throw e;
      }
    }
    return inputStream;
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    LOG.warn("rename is not supported when using JDBCUnderFileSystem, returning false.");
    return false;
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    LOG.warn("rename is not supported when using JDBCUnderFileSystem, returning false.");
    return false;
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    HashMap<String, String> properties = new HashMap<>(mUfsConf.getUserSpecifiedConf());
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

  // Not supported
  @Override
  public void setOwner(String path, String user, String group) {
    LOG.debug("setOwner not supported in JDBCUnderFileSystem");
  }

  // Not supported
  @Override
  public void setMode(String path, short mode) throws IOException {
    LOG.debug("setMode not supported in JDBCUnderFileSystem");
  }

  @Override
  public boolean supportsFlush() {
    return false;
  }
}
