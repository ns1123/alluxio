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
import alluxio.underfs.UnderFileSystem;
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

  private final String mUfsPrefix;

  /**
   * Constructs a new HDFS {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param configuration the configuration for Alluxio
   * @param conf the configuration for Hadoop
   */
  public HdfsUnderFileSystem(AlluxioURI uri, Configuration configuration, Object conf) {
    super(uri, configuration);
    mUfsPrefix = uri.toString();
    org.apache.hadoop.conf.Configuration tConf;
    if (conf != null && conf instanceof org.apache.hadoop.conf.Configuration) {
      tConf = (org.apache.hadoop.conf.Configuration) conf;
    } else {
      tConf = new org.apache.hadoop.conf.Configuration();
    }
    prepareConfiguration(mUfsPrefix, configuration, tConf);
    tConf.addResource(new Path(tConf.get(Constants.UNDERFS_HDFS_CONFIGURATION)));
    HdfsUnderFileSystemUtils.addS3Credentials(tConf);

    // ENTERPRISE ADD
    if (tConf.get("hadoop.security.authentication").equalsIgnoreCase("KERBEROS")) {
      try {
        // NOTE: this is temporary solution before Client/Worker decoupling. After that, there is
        // no need to distinguish server-side and client-side connection to secure HDFS as UFS.
        // It's brittle to depend on alluxio.logger.type.
        // TODO(chaomin): add UFS delegation so that Alluxio Master and Worker can access secure
        // HDFS on behalf of Alluxio client user.
        String loggerType = configuration.get(Constants.LOGGER_TYPE);
        if (loggerType.equalsIgnoreCase("MASTER_LOGGER")) {
          LOG.info("connectFromMaster");
          connectFromMaster(configuration, NetworkAddressUtils.getConnectHost(
              NetworkAddressUtils.ServiceType.MASTER_RPC, configuration));
        } else  if (loggerType.equalsIgnoreCase("WORKER_LOGGER")) {
          LOG.info("connectFromWorker");
          connectFromWorker(configuration, NetworkAddressUtils.getConnectHost(
              NetworkAddressUtils.ServiceType.WORKER_RPC, configuration));
        } else {
          LOG.info("connectFromClient");
          connectFromAlluxioClient(configuration);
        }
      } catch (IOException e) {
        LOG.error("Login error : " + e);
      }
    }
    // ENTERPRISE END

    Path path = new Path(mUfsPrefix);
    try {
      mFileSystem = path.getFileSystem(tConf);
    } catch (IOException e) {
      LOG.error("Exception thrown when trying to get FileSystem for {}", mUfsPrefix, e);
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
  // ENTERPRISE EDIT
  public FSDataOutputStream create(final String path) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<FSDataOutputStream>() {
          @Override
          public FSDataOutputStream run() throws IOException {
            IOException te = null;
            RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
            while (retryPolicy.attemptRetry()) {
              try {
                LOG.debug("Creating HDFS file at {}", path);
                return FileSystem.create(mFileSystem, new Path(path), PERMISSION);
              } catch (IOException e) {
                LOG.error("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage(), e);
                te = e;
              }
            }
            throw te;
          }
        });
  }
  // ENTERPRISE REPLACES
  // public FSDataOutputStream create(String path) throws IOException {
  //   IOException te = null;
  //   RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
  //   while (retryPolicy.attemptRetry()) {
  //     try {
  //       LOG.debug("Creating HDFS file at {}", path);
  //       return FileSystem.create(mFileSystem, new Path(path), PERMISSION);
  //     } catch (IOException e) {
  //       LOG.error("Retry count {} : {} ", retryPolicy.getRetryCount(), e.getMessage(), e);
  //       te = e;
  //     }
  //   }
  //   throw te;
  // }
  // ENTERPRISE END

  /**
   * Creates a new file.
   *
   * @param path the path
   * @param blockSizeByte the size of the block in bytes; should be a multiple of 512
   * @return a {@code FSDataOutputStream} object
   * @throws IOException when a non-Alluxio related exception occurs
   */
  @Override
  public FSDataOutputStream create(String path, int blockSizeByte) throws IOException {
    // TODO(hy): Fix this.
    // return create(path, (short) Math.min(3, mFileSystem.getDefaultReplication()),
    // blockSizeBytes);
    return create(path);
  }

  @Override
  public FSDataOutputStream create(String path, short replication, int blockSizeByte)
      throws IOException {
    // TODO(hy): Fix this.
    // return create(path, (short) Math.min(3, mFileSystem.getDefaultReplication()),
    // blockSizeBytes);
    return create(path);
    // LOG.info("{} {} {}", path, replication, blockSizeBytes);
    // IOException te = null;
    // int cnt = 0;
    // while (cnt < MAX_TRY) {
    // try {
    // return mFileSystem.create(new Path(path), true, 4096, replication, blockSizeBytes);
    // } catch (IOException e) {
    // cnt++;
    // LOG.error("{} : {}", cnt, e.getMessage(), e);
    // te = e;
    // continue;
    // }
    // }
    // throw te;
  }

  @Override
  // ENTERPRISE EDIT
  public boolean delete(final String path, final boolean recursive) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Boolean>() {
          @Override
          public Boolean run() throws IOException {
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
        });
  }
  // ENTERPRISE REPLACES
  // public boolean delete(String path, boolean recursive) throws IOException {
  //   LOG.debug("deleting {} {}", path, recursive);
  //   IOException te = null;
  //   RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
  //   while (retryPolicy.attemptRetry()) {
  //     try {
  //       return mFileSystem.delete(new Path(path), recursive);
  //     } catch (IOException e) {
  //       LOG.error("Retry count {} : {}", retryPolicy.getRetryCount(), e.getMessage(), e);
  //       te = e;
  //     }
  //   }
  //   throw te;
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public boolean exists(final String path) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Boolean>() {
          @Override
          public Boolean run() throws IOException {
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
        });
  }
  // ENTERPRISE REPLACES
  // public boolean exists(String path) throws IOException {
  //   IOException te = null;
  //   RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
  //   while (retryPolicy.attemptRetry()) {
  //     try {
  //       return mFileSystem.exists(new Path(path));
  //     } catch (IOException e) {
  //       LOG.error("{} try to check if {} exists : {}", retryPolicy.getRetryCount(), path,
  //           e.getMessage(), e);
  //       te = e;
  //     }
  //   }
  //   throw te;
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public long getBlockSizeByte(final String path) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Long>() {
          @Override
          public Long run() throws IOException {
            Path tPath = new Path(path);
            if (!mFileSystem.exists(tPath)) {
              throw new FileNotFoundException(path);
            }
            FileStatus fs = mFileSystem.getFileStatus(tPath);
            return fs.getBlockSize();
          }
        });
  }
  // ENTERPRISE REPLACES
  // public long getBlockSizeByte(String path) throws IOException {
  //   Path tPath = new Path(path);
  //   if (!mFileSystem.exists(tPath)) {
  //     throw new FileNotFoundException(path);
  //   }
  //   FileStatus fs = mFileSystem.getFileStatus(tPath);
  //   return fs.getBlockSize();
  // }
  // ENTERPRISE END

  @Override
  public Object getConf() {
    return mFileSystem.getConf();
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return getFileLocations(path, 0);
  }

  @Override
  // ENTERPRISE EDIT
  public List<String> getFileLocations(final String path, final long offset) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<List<String>>() {
          @Override
          public List<String> run() throws IOException {
            List<String> ret = new ArrayList<String>();
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
        });
  }
  // ENTERPRISE REPLACES
  // public List<String> getFileLocations(String path, long offset) throws IOException {
  //   List<String> ret = new ArrayList<String>();
  //   try {
  //     FileStatus fStatus = mFileSystem.getFileStatus(new Path(path));
  //     BlockLocation[] bLocations = mFileSystem.getFileBlockLocations(fStatus, offset, 1);
  //     if (bLocations.length > 0) {
  //       String[] names = bLocations[0].getNames();
  //       Collections.addAll(ret, names);
  //     }
  //   } catch (IOException e) {
  //     LOG.error("Unable to get file location for {}", path, e);
  //   }
  //   return ret;
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public long getFileSize(final String path) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Long>() {
          @Override
          public Long run() throws IOException {
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
            return -1L;
          }
        });
  }
  // ENTERPRISE REPLACES
  // public long getFileSize(String path) throws IOException {
  //   Path tPath = new Path(path);
  //   RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
  //   while (retryPolicy.attemptRetry()) {
  //     try {
  //       FileStatus fs = mFileSystem.getFileStatus(tPath);
  //       return fs.getLen();
  //     } catch (IOException e) {
  //       LOG.error("{} try to get file size for {} : {}", retryPolicy.getRetryCount(), path,
  //           e.getMessage(), e);
  //     }
  //   }
  //   return -1;
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public long getModificationTimeMs(final String path) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Long>() {
          @Override
          public Long run() throws IOException {
            Path tPath = new Path(path);
            if (!mFileSystem.exists(tPath)) {
              throw new FileNotFoundException(path);
            }
            FileStatus fs = mFileSystem.getFileStatus(tPath);
            return fs.getModificationTime();
          }
        });
  }
  // ENTERPRISE REPLACES
  // public long getModificationTimeMs(String path) throws IOException {
  //   Path tPath = new Path(path);
  //   if (!mFileSystem.exists(tPath)) {
  //     throw new FileNotFoundException(path);
  //   }
  //   FileStatus fs = mFileSystem.getFileStatus(tPath);
  //   return fs.getModificationTime();
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public long getSpace(String path, final SpaceType type) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Long>() {
          @Override
          public Long run() throws IOException {
            // Ignoring the path given, will give information for entire cluster
            // as Alluxio can load/store data out of entire HDFS cluster
            if (mFileSystem instanceof DistributedFileSystem) {
              switch (type) {
                case SPACE_TOTAL:
                  return ((DistributedFileSystem) mFileSystem).getDiskStatus().getCapacity();
                case SPACE_USED:
                  return ((DistributedFileSystem) mFileSystem).getDiskStatus().getDfsUsed();
                case SPACE_FREE:
                  return ((DistributedFileSystem) mFileSystem).getDiskStatus().getRemaining();
                default:
                  throw new IOException("Unknown getSpace parameter: " + type);
              }
            }
            return -1L;
          }
        });
  }
  // ENTERPRISE REPLACES
  // public long getSpace(String path, SpaceType type) throws IOException {
  //   // Ignoring the path given, will give information for entire cluster
  //   // as Alluxio can load/store data out of entire HDFS cluster
  //   if (mFileSystem instanceof DistributedFileSystem) {
  //     switch (type) {
  //       case SPACE_TOTAL:
  //         return ((DistributedFileSystem) mFileSystem).getDiskStatus().getCapacity();
  //       case SPACE_USED:
  //         return ((DistributedFileSystem) mFileSystem).getDiskStatus().getDfsUsed();
  //       case SPACE_FREE:
  //         return ((DistributedFileSystem) mFileSystem).getDiskStatus().getRemaining();
  //       default:
  //         throw new IOException("Unknown getSpace parameter: " + type);
  //     }
  //   }
  //   return -1;
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public boolean isFile(final String path) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Boolean>() {
          @Override
          public Boolean run() throws IOException {
            return mFileSystem.isFile(new Path(path));
          }
        });
  }
  // ENTERPRISE REPLACES
  // public boolean isFile(String path) throws IOException {
  //   return mFileSystem.isFile(new Path(path));
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public String[] list(final String path) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<String[]>() {
          @Override
          public String[] run() throws IOException {
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
                rtn[i++] = status.getPath().getName();
              }
              return rtn;
            } else {
              return null;
            }
          }
        });
  }
  // ENTERPRISE REPLACES
  // public String[] list(String path) throws IOException {
  //   FileStatus[] files;
  //   try {
  //     files = mFileSystem.listStatus(new Path(path));
  //   } catch (FileNotFoundException e) {
  //     return null;
  //   }
  //   if (files != null && !isFile(path)) {
  //     String[] rtn = new String[files.length];
  //     int i = 0;
  //     for (FileStatus status : files) {
  //       // only return the relative path, to keep consistent with java.io.File.list()
  //       rtn[i++] =  status.getPath().getName();
  //     }
  //     return rtn;
  //   } else {
  //     return null;
  //   }
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE ADD
  // TODO(chaomin): make connectFromMaster private.
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
  // TODO(chaomin): make connectFromWorker private.
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
    // ENTERPRISE ADD
    LOG.info("logging in HDFS principal = {}, keytabFile = {} hostname = {}",
        principal, keytabFile, hostname);
    // ENTERPRISE END
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    // ENTERPRISE EDIT
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.set("hadoop.security.authentication", "KERBEROS");

    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
    // ENTERPRISE REPLACES
    // conf.set(keytabFileKey, keytabFile);
    // conf.set(principalKey, principal);
    // SecurityUtil.login(conf, keytabFileKey, principalKey, hostname);
    // ENTERPRISE END
  }

  @Override
  // ENTERPRISE EDIT
  public boolean mkdirs(final String path, boolean createParent) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Boolean>() {
          @Override
          public Boolean run() throws IOException {
            IOException te = null;
            RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
            while (retryPolicy.attemptRetry()) {
              try {
                Path hdfsPath = new Path(path);
                if (mFileSystem.exists(hdfsPath)) {
                  LOG.debug("Trying to create existing directory at {}", path);
                  return false;
                }
                // Create directories one by one with explicit permissions to ensure no umask is
                // applied, using mkdirs will apply the permission only to the last directory
                Stack<Path> dirsToMake = new Stack<Path>();
                dirsToMake.push(hdfsPath);
                Path parent = hdfsPath.getParent();
                while (!mFileSystem.exists(parent)) {
                  dirsToMake.push(parent);
                  parent = parent.getParent();
                }
                while (!dirsToMake.empty()) {
                  if (!FileSystem.mkdirs(mFileSystem, dirsToMake.pop(), PERMISSION)) {
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
        });
  }
  // ENTERPRISE REPLACES
  // public boolean mkdirs(String path, boolean createParent) throws IOException {
  //   IOException te = null;
  //   RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
  //   while (retryPolicy.attemptRetry()) {
  //     try {
  //       Path hdfsPath = new Path(path);
  //       if (mFileSystem.exists(hdfsPath)) {
  //         LOG.debug("Trying to create existing directory at {}", path);
  //         return false;
  //       }
  //       // Create directories one by one with explicit permissions to ensure no umask is applied,
  //       // using mkdirs will apply the permission only to the last directory
  //       Stack<Path> dirsToMake = new Stack<Path>();
  //       dirsToMake.push(hdfsPath);
  //       Path parent = hdfsPath.getParent();
  //       while (!mFileSystem.exists(parent)) {
  //         dirsToMake.push(parent);
  //         parent = parent.getParent();
  //       }
  //       while (!dirsToMake.empty()) {
  //         if (!FileSystem.mkdirs(mFileSystem, dirsToMake.pop(), PERMISSION)) {
  //           return false;
  //         }
  //       }
  //       return true;
  //     } catch (IOException e) {
  //       LOG.error("{} try to make directory for {} : {}", retryPolicy.getRetryCount(), path,
  //           e.getMessage(), e);
  //       te = e;
  //     }
  //   }
  //   throw te;
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public FSDataInputStream open(final String path) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<FSDataInputStream>() {
          @Override
          public FSDataInputStream run() throws IOException {
            IOException te = null;
            RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
            while (retryPolicy.attemptRetry()) {
              try {
                return mFileSystem.open(new Path(path));
              } catch (IOException e) {
                LOG.error("{} try to open {} : {}", retryPolicy.getRetryCount(), path,
                    e.getMessage(), e);
                te = e;
              }
            }
            throw te;
          }
        });
  }
  // ENTERPRISE REPLACES
  // public FSDataInputStream open(String path) throws IOException {
  //   IOException te = null;
  //   RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
  //   while (retryPolicy.attemptRetry()) {
  //     try {
  //       return mFileSystem.open(new Path(path));
  //     } catch (IOException e) {
  //       LOG.error("{} try to open {} : {}", retryPolicy.getRetryCount(), path, e.getMessage(),
  //           e);
  //       te = e;
  //     }
  //   }
  //   throw te;
  // }
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public boolean rename(final String src, final String dst) throws IOException {
    return HdfsSecurityUtils
        .runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Boolean>() {
          @Override
          public Boolean run() throws IOException {
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
        });
  }
  // ENTERPRISE REPLACES
  // public boolean rename(String src, String dst) throws IOException {
  //   LOG.debug("Renaming from {} to {}", src, dst);
  //   if (!exists(src)) {
  //     LOG.error("File {} does not exist. Therefore rename to {} failed.", src, dst);
  //     return false;
  //   }
  //
  //   if (exists(dst)) {
  //     LOG.error("File {} does exist. Therefore rename from {} failed.", dst, src);
  //     return false;
  //   }
  //
  //   IOException te = null;
  //   RetryPolicy retryPolicy = new CountingRetry(MAX_TRY);
  //   while (retryPolicy.attemptRetry()) {
  //     try {
  //       return mFileSystem.rename(new Path(src), new Path(dst));
  //     } catch (IOException e) {
  //       LOG.error("{} try to rename {} to {} : {}", retryPolicy.getRetryCount(), src, dst,
  //           e.getMessage(), e);
  //      te = e;
  //     }
  //   }
  //   throw te;
  // }
  // ENTERPRISE END

  @Override
  public void setConf(Object conf) {
    mFileSystem.setConf((org.apache.hadoop.conf.Configuration) conf);
  }

  @Override
  // ENTERPRISE EDIT
  public void setOwner(final String path, final String user, final String group)
      throws IOException {
    HdfsSecurityUtils.runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Void>() {
      @Override
      public Void run() throws IOException {
        try {
          FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
          LOG.info("Changing file '{}' user from: {} to {}, group from: {} to {}",
              fileStatus.getPath(), fileStatus.getOwner(), user, fileStatus.getGroup(), group);
          mFileSystem.setOwner(fileStatus.getPath(), user, group);
          return null;
        } catch (IOException e) {
          LOG.error("Fail to set owner for {} with user: {}, group: {}", path, user, group, e);
          LOG.warn("In order for Alluxio to create HDFS files with the correct user and groups, "
              + "Alluxio should be added to the HDFS superusers.");
          throw e;
        }
      }
    });
  }
  // ENTERPRISE REPLACES
  // public void setOwner(String path, String user, String group) throws IOException {
  //  try {
  //    FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
  //    LOG.info("Changing file '{}' user from: {} to {}, group from: {} to {}", fileStatus
  //        .getPath(),
  //        fileStatus.getOwner(), user, fileStatus.getGroup(), group);
  //    mFileSystem.setOwner(fileStatus.getPath(), user, group);
  //  } catch (IOException e) {
  //    LOG.error("Fail to set owner for {} with user: {}, group: {}", path, user, group, e);
  //    LOG.warn("In order for Alluxio to create HDFS files with the correct user and groups, "
  //        + "Alluxio should be added to the HDFS superusers.");
  //    throw e;
  //  }
  // }
  //
  // ENTERPRISE END

  @Override
  // ENTERPRISE EDIT
  public void setPermission(final String path, final String posixPerm) throws IOException {
    HdfsSecurityUtils.runAsCurrentUser(new HdfsSecurityUtils.AlluxioSecuredRunner<Void>() {
      @Override
      public Void run() throws IOException {
        try {
          FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
          LOG.info("Changing file '{}' permissions from: {} to {}", fileStatus.getPath(),
              fileStatus.getPermission(), posixPerm);
          FsPermission perm = new FsPermission(Short.parseShort(posixPerm));
          mFileSystem.setPermission(fileStatus.getPath(), perm);
          return null;
        } catch (IOException e) {
          LOG.error("Fail to set permission for {} with perm {}", path, posixPerm, e);
          throw e;
        }
      }
    });
  }
  // ENTERPRISE REPLACES
  // public void setPermission(String path, String posixPerm) throws IOException {
  //   try {
  //     FileStatus fileStatus = mFileSystem.getFileStatus(new Path(path));
  //     LOG.info("Changing file '{}' permissions from: {} to {}", fileStatus.getPath(),
  //         fileStatus.getPermission(), posixPerm);
  //     FsPermission perm = new FsPermission(Short.parseShort(posixPerm));
  //     mFileSystem.setPermission(fileStatus.getPath(), perm);
  //   } catch (IOException e) {
  //     LOG.error("Fail to set permission for {} with perm {}", path, posixPerm, e);
  //     throw e;
  //   }
  // }
  // ENTERPRISE END
}
