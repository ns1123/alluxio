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

package alluxio.underfs.hdfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.AuthType;
import alluxio.security.authorization.Permission;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
// ENTERPRISE ADD
import alluxio.util.network.NetworkAddressUtils;
// ENTERPRISE END

import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
// ENTERPRISE EDIT
import org.apache.hadoop.security.UserGroupInformation;
// ENTERPRISE REPLACES
// import org.apache.hadoop.security.SecurityUtil;
// ENTERPRISE END
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import javax.annotation.concurrent.ThreadSafe;

/**
 * HDFS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class HdfsUnderFileSystem extends UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int MAX_TRY = 5;
  // TODO(hy): Add a sticky bit and narrow down the permission in hadoop 2.
  private static final FsPermission PERMISSION = new FsPermission((short) 0777)
      .applyUMask(FsPermission.createImmutable((short) 0000));

  // ENTERPRISE EDIT
  private FileSystem mFileSystem;
  // ENTERPRISE REPLACES
  // private final FileSystem mFileSystem;
  // ENTERPRISE END

  /**
   * Constructs a new HDFS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param configuration the configuration for Alluxio
   * @param conf the configuration for Hadoop
   */
  public HdfsUnderFileSystem(AlluxioURI uri, Configuration configuration, Object conf) {
    super(uri, configuration);
    // ENTERPRISE EDIT
    final String ufsPrefix = uri.toString();
    final org.apache.hadoop.conf.Configuration tConf;
    // ENTERPRISE REPLACES
    // String ufsPrefix = uri.toString();
    // org.apache.hadoop.conf.Configuration tConf;
    // ENTERPRISE END

    if (conf != null && conf instanceof org.apache.hadoop.conf.Configuration) {
      tConf = (org.apache.hadoop.conf.Configuration) conf;
    } else {
      tConf = new org.apache.hadoop.conf.Configuration();
    }
    prepareConfiguration(ufsPrefix, configuration, tConf);
    tConf.addResource(new Path(tConf.get(Constants.UNDERFS_HDFS_CONFIGURATION)));
    HdfsUnderFileSystemUtils.addS3Credentials(tConf);

    // ENTERPRISE ADD
    if (tConf.get("hadoop.security.authentication").equalsIgnoreCase(
        AuthType.KERBEROS.getAuthName())) {
      String loggerType = configuration.get(Constants.LOGGER_TYPE);
      try {
        // NOTE: this is temporary solution with Client/Worker decoupling turned off. Once the
        // decoupling is enabled by default, there is no need to distinguish server-side and
        // client-side connection to secure HDFS as UFS.
        // TODO(chaomin): consider adding a JVM-level constant to distinguish between Alluxio server
        // and client. It's brittle to depend on alluxio.logger.type.
        if (loggerType.equalsIgnoreCase("MASTER_LOGGER")) {
          connectFromMaster(configuration, NetworkAddressUtils.getConnectHost(
              NetworkAddressUtils.ServiceType.MASTER_RPC, configuration));
        } else if (loggerType.equalsIgnoreCase("WORKER_LOGGER")) {
          connectFromWorker(configuration, NetworkAddressUtils.getConnectHost(
              NetworkAddressUtils.ServiceType.WORKER_RPC, configuration));
        } else {
          connectFromAlluxioClient(configuration);
        }
      } catch (IOException e) {
        LOG.error("Login error: " + e);
      }

      try {
        if ((loggerType.equalsIgnoreCase("MASTER_LOGGER")
            || loggerType.equalsIgnoreCase("WORKER_LOGGER")) && !mUser.isEmpty()) {
          // Use HDFS super-user proxy feature to make Alluxio server act as the end-user.
          // The Alluxio server user must be configured as a superuser proxy in HDFS configuration.
          UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser(mUser,
              UserGroupInformation.getLoginUser());
          LOG.debug("Using proxyUgi: {}", proxyUgi.toString());
          HdfsSecurityUtils.runAs(proxyUgi, new HdfsSecurityUtils.SecuredRunner<Void>() {
            @Override
            public Void run() throws IOException {
              Path path = new Path(ufsPrefix);
              mFileSystem = path.getFileSystem(tConf);
              return null;
            }
          });
        } else {
          // Alluxio client runs HDFS operations as the current user.
          HdfsSecurityUtils.runAsCurrentUser(new HdfsSecurityUtils.SecuredRunner<Void>() {
            @Override
            public Void run() throws IOException {
              Path path = new Path(ufsPrefix);
              mFileSystem = path.getFileSystem(tConf);
              return null;
            }
          });
        }
      } catch (IOException e) {
        LOG.error("Exception thrown when trying to get FileSystem for {}", ufsPrefix, e);
        throw Throwables.propagate(e);
      }
      return;
    }
    // ENTERPRISE END
    Path path = new Path(ufsPrefix);
    try {
      mFileSystem = path.getFileSystem(tConf);
    } catch (IOException e) {
      LOG.error("Exception thrown when trying to get FileSystem for {}", ufsPrefix, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public UnderFSType getUnderFSType() {
    return UnderFSType.HDFS;
  }

  /**
   * Prepares the Hadoop configuration necessary to successfully obtain a {@link FileSystem}
   * instance that can access the provided path.
   * <p>
   * Derived implementations that work with specialised Hadoop {@linkplain FileSystem} API
   * compatible implementations can override this method to add implementation specific
   * configuration necessary for obtaining a usable {@linkplain FileSystem} instance.
   * </p>
   *
   * @param path file system path
   * @param conf Alluxio Configuration
   * @param hadoopConf Hadoop configuration
   */
  protected void prepareConfiguration(String path, Configuration conf,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    // On Hadoop 2.x this is strictly unnecessary since it uses ServiceLoader to automatically
    // discover available file system implementations. However this configuration setting is
    // required for earlier Hadoop versions plus it is still honoured as an override even in 2.x so
    // if present propagate it to the Hadoop configuration
    String ufsHdfsImpl = mConfiguration.get(Constants.UNDERFS_HDFS_IMPL);
    if (!StringUtils.isEmpty(ufsHdfsImpl)) {
      hadoopConf.set("fs.hdfs.impl", ufsHdfsImpl);
    }

    // To disable the instance cache for hdfs client, otherwise it causes the
    // FileSystem closed exception. Being configurable for unit/integration
    // test only, and not expose to the end-user currently.
    hadoopConf.set("fs.hdfs.impl.disable.cache",
        System.getProperty("fs.hdfs.impl.disable.cache", "false"));

    HdfsUnderFileSystemUtils.addKey(hadoopConf, conf, Constants.UNDERFS_HDFS_CONFIGURATION);
  }

  @Override
  public void close() throws IOException {
    // Don't close; file systems are singletons and closing it here could break other users
  }

  @Override
  public FSDataOutputStream create(String path) throws IOException {
    return create(path, new CreateOptions(mConfiguration));
  }

  @Override
  public FSDataOutputStream create(String path, CreateOptions options)
      throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    Permission perm = options.getPermission();
    while (retryPolicy.attemptRetry()) {
      try {
        LOG.debug("Creating HDFS file at {} with perm {}", path, perm.toString());
        // TODO(chaomin): support creating HDFS files with specified block size and replication.
        return FileSystem.create(mFileSystem, new Path(path),
            new FsPermission(perm.getMode().toShort()));
      } catch (IOException e) {
        LOG.error("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    LOG.debug("deleting {} {}", path, recursive);
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.delete(new Path(path), recursive);
      } catch (IOException e) {
        LOG.error("Retry count {} : {}", retryPolicy.getRetryCount(), e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean exists(String path) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.exists(new Path(path));
      } catch (IOException e) {
        LOG.error("{} try to check if {} exists : {}", retryPolicy.getRetryCount(), path,
            e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    Path tPath = new Path(path);
    if (!mFileSystem.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    return fs.getBlockSize();
  }

  @Override
  public Object getConf() {
    return mFileSystem.getConf();
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return getFileLocations(path, 0);
  }

  @Override
  public List<String> getFileLocations(String path, long offset) throws IOException {
    List<String> ret = new ArrayList<>();
    try {
      FileStatus fStatus = mFileSystem.getFileStatus(new Path(path));
      BlockLocation[] bLocations = mFileSystem.getFileBlockLocations(fStatus, offset, 1);
      if (bLocations.length > 0) {
        String[] names = bLocations[0].getNames();
        Collections.addAll(ret, names);
      }
    } catch (IOException e) {
      LOG.error("Unable to get file location for {}", path, e);
    }
    return ret;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    Path tPath = new Path(path);
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        FileStatus fs = mFileSystem.getFileStatus(tPath);
        return fs.getLen();
      } catch (IOException e) {
        LOG.error("{} try to get file size for {} : {}", retryPolicy.getRetryCount(), path,
            e.getMessage(), e);
      }
    }
    return -1;
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    Path tPath = new Path(path);
    if (!mFileSystem.exists(tPath)) {
      throw new FileNotFoundException(path);
    }
    FileStatus fs = mFileSystem.getFileStatus(tPath);
    return fs.getModificationTime();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    // Ignoring the path given, will give information for entire cluster
    // as Alluxio can load/store data out of entire HDFS cluster
    if (mFileSystem instanceof DistributedFileSystem) {
      switch (type) {
        case SPACE_TOTAL:
          // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
          // FileSystem.getStatus().getCapacity() will be the new one.
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getCapacity();
        case SPACE_USED:
          // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
          // FileSystem.getStatus().getUsed() will be the new one.
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getDfsUsed();
        case SPACE_FREE:
          // Due to Hadoop 1 support we stick with the deprecated version. If we drop support for it
          // FileSystem.getStatus().getRemaining() will be the new one.
          return ((DistributedFileSystem) mFileSystem).getDiskStatus().getRemaining();
        default:
          throw new IOException("Unknown getSpace parameter: " + type);
      }
    }
    return -1;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mFileSystem.isFile(new Path(path));
  }

  @Override
  public String[] list(String path) throws IOException {
    FileStatus[] files;
    try {
      files = mFileSystem.listStatus(new Path(path));
    } catch (FileNotFoundException e) {
      return null;
    }
    if (files != null && !isFile(path)) {
      String[] rtn = new String[files.length];
      int i = 0;
      for (FileStatus status : files) {
        // only return the relative path, to keep consistent with java.io.File.list()
        rtn[i++] =  status.getPath().getName();
      }
      return rtn;
    } else {
      return null;
    }
  }

  @Override
  // ENTERPRISE ADD
  // TODO(chaomin): make connectFromMaster private and deprecate it.
  // ENTERPRISE END
  public void connectFromMaster(Configuration conf, String host) throws IOException {
    // ENTERPRISE EDIT
    connectFromAlluxioServer(conf, host);
    // ENTERPRISE REPLACES
    // if (!conf.containsKey(Constants.MASTER_KEYTAB_KEY)
    //     || !conf.containsKey(Constants.MASTER_PRINCIPAL_KEY)) {
    //   return;
    // }
    // String masterKeytab = conf.get(Constants.MASTER_KEYTAB_KEY);
    // String masterPrincipal = conf.get(Constants.MASTER_PRINCIPAL_KEY);
    //
    // login(Constants.MASTER_KEYTAB_KEY, masterKeytab, Constants.MASTER_PRINCIPAL_KEY,
    //     masterPrincipal, host);
    // ENTERPRISE END
  }

  @Override
  // ENTERPRISE ADD
  // TODO(chaomin): make connectFromWorker private and deprecate it.
  // ENTERPRISE END
  public void connectFromWorker(Configuration conf, String host) throws IOException {
    // ENTERPRISE EDIT
    connectFromAlluxioServer(conf, host);
    // ENTERPRISE REPLACES
    // if (!conf.containsKey(Constants.WORKER_KEYTAB_KEY)
    //     || !conf.containsKey(Constants.WORKER_PRINCIPAL_KEY)) {
    //   return;
    // }
    // String workerKeytab = conf.get(Constants.WORKER_KEYTAB_KEY);
    // String workerPrincipal = conf.get(Constants.WORKER_PRINCIPAL_KEY);
    //
    // login(Constants.WORKER_KEYTAB_KEY, workerKeytab, Constants.WORKER_PRINCIPAL_KEY,
    //     workerPrincipal, host);
    // ENTERPRISE END
  }

  // ENTERPRISE ADD
  private void connectFromAlluxioServer(Configuration conf, String host) throws IOException {
    if (!conf.containsKey(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL)
        || !conf.containsKey(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE)) {
      return;
    }
    String principal = conf.get(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL);
    String keytab = conf.get(Constants.SECURITY_KERBEROS_SERVER_KEYTAB_FILE);

    login(principal, keytab, host);
  }

  private void connectFromAlluxioClient(Configuration conf) throws IOException {
    if (!conf.containsKey(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL)
        || !conf.containsKey(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE)) {
      return;
    }
    String principal = conf.get(Constants.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
    String keytab = conf.get(Constants.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);

    login(principal, keytab, null);
  }
  // ENTERPRISE END

  private void login(String principal, String keytabFile, String hostname) throws IOException {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    // ENTERPRISE EDIT
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.set("hadoop.security.authentication", AuthType.KERBEROS.getAuthName());

    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
    // ENTERPRISE REPLACES
    // conf.set(keytabFileKey, keytabFile);
    // conf.set(principalKey, principal);
    // SecurityUtil.login(conf, keytabFileKey, principalKey, hostname);
    // ENTERPRISE END
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    return mkdirs(path, new MkdirsOptions(mConfiguration).setCreateParent(createParent));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        Path hdfsPath = new Path(path);
        if (mFileSystem.exists(hdfsPath)) {
          LOG.debug("Trying to create existing directory at {}", path);
          return false;
        }
        // Create directories one by one with explicit permissions to ensure no umask is applied,
        // using mkdirs will apply the permission only to the last directory
        Stack<Path> dirsToMake = new Stack<>();
        dirsToMake.push(hdfsPath);
        Path parent = hdfsPath.getParent();
        while (!mFileSystem.exists(parent)) {
          dirsToMake.push(parent);
          parent = parent.getParent();
        }
        while (!dirsToMake.empty()) {
          if (!FileSystem.mkdirs(mFileSystem, dirsToMake.pop(),
              new FsPermission(options.getPermission().getMode().toShort()))) {
            return false;
          }
        }
        return true;
      } catch (IOException e) {
        LOG.error("{} try to make directory for {} : {}", retryPolicy.getRetryCount(), path,
            e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public FSDataInputStream open(String path) throws IOException {
    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.open(new Path(path));
      } catch (IOException e) {
        LOG.error("{} try to open {} : {}", retryPolicy.getRetryCount(), path, e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    LOG.debug("Renaming from {} to {}", src, dst);
    if (!exists(src)) {
      LOG.error("File {} does not exist. Therefore rename to {} failed.", src, dst);
      return false;
    }

    if (exists(dst)) {
      LOG.error("File {} does exist. Therefore rename from {} failed.", dst, src);
      return false;
    }

    IOException te = null;
    RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
    while (retryPolicy.attemptRetry()) {
      try {
        return mFileSystem.rename(new Path(src), new Path(dst));
      } catch (IOException e) {
        LOG.error("{} try to rename {} to {} : {}", retryPolicy.getRetryCount(), src, dst,
            e.getMessage(), e);
        te = e;
      }
    }
    throw te;
  }

  @Override
  public void setConf(Object conf) {
    mFileSystem.setConf((org.apache.hadoop.conf.Configuration) conf);
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      LOG.info("Changing file '{}' user from: {} to {}, group from: {} to {}", fileStatus.getPath(),
          fileStatus.getOwner(), user, fileStatus.getGroup(), group);
      mFileSystem.setOwner(fileStatus.getPath(), user, group);
    } catch (IOException e) {
      LOG.error("Fail to set owner for {} with user: {}, group: {}", path, user, group, e);
      LOG.warn("In order for Alluxio to create HDFS files with the correct user and groups, "
          + "Alluxio should be added to the HDFS superusers.");
      throw e;
    }
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    try {
      FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
      LOG.info("Changing file '{}' permissions from: {} to {}", fileStatus.getPath(),
          fileStatus.getPermission(), mode);
      mFileSystem.setPermission(fileStatus.getPath(), new FsPermission(mode));
    } catch (IOException e) {
      LOG.error("Fail to set permission for {} with perm {}", path, mode, e);
      throw e;
    }
  }

  @Override
  public String getOwner(String path) throws IOException {
    try {
      return mFileSystem.getFileStatus(new Path(path)).getOwner();
    } catch (IOException e) {
      LOG.error("Fail to get owner for {} ", path, e);
      throw e;
    }
  }

  @Override
  public String getGroup(String path) throws IOException {
    try {
      return mFileSystem.getFileStatus(new Path(path)).getGroup();
    } catch (IOException e) {
      LOG.error("Fail to get group for {} ", path, e);
      throw e;
    }
  }

  @Override
  public short getMode(String path) throws IOException {
    try {
      return mFileSystem.getFileStatus(new Path(path)).getPermission().toShort();
    } catch (IOException e) {
      LOG.error("Fail to get permission for {} ", path, e);
      throw e;
    }
  }

}
