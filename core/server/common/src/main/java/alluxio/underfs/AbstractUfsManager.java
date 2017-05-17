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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.IdUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Basic implementation of {@link UfsManager}.
 */
public abstract class AbstractUfsManager implements UfsManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractUfsManager.class);

  /**
   * The key of the UFS cache.
   */
  public static class Key {
    private final String mScheme;
    private final String mAuthority;
    private final Map<String, String> mProperties;
    // ALLUXIO CS ADD
    private final String mOwner;
    private final String mGroup;
    // ALLUXIO CS END

    Key(AlluxioURI uri, Map<String, String> properties) {
      mScheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
      mAuthority = uri.getAuthority() == null ? "" : uri.getAuthority().toLowerCase();
      mProperties = (properties == null || properties.isEmpty()) ? null : properties;
      // ALLUXIO CS ADD
      if (alluxio.util.CommonUtils.isAlluxioServer()) {
        mOwner = alluxio.util.SecurityUtils.getOwnerFromThriftClient();
        mGroup = alluxio.util.SecurityUtils.getGroupFromThriftClient();
      } else {
        mOwner = alluxio.util.SecurityUtils.getOwnerFromLoginModule();
        mGroup = alluxio.util.SecurityUtils.getGroupFromLoginModule();
      }
      // ALLUXIO CS END
    }

    @Override
    public int hashCode() {
      // ALLUXIO CS REPLACE
      // return Objects.hashCode(mScheme, mAuthority, mProperties);
      // ALLUXIO CS WITH
      return Objects.hashCode(mScheme, mAuthority, mProperties, mOwner, mGroup);
      // ALLUXIO CS END
    }

    @Override
    public boolean equals(Object object) {
      if (object == this) {
        return true;
      }

      if (!(object instanceof Key)) {
        return false;
      }

      Key that = (Key) object;
      // ALLUXIO CS REPLACE
      // return Objects.equal(mAuthority, that.mAuthority) && Objects
      //     .equal(mProperties, that.mProperties) && Objects.equal(mScheme, that.mScheme);
      // ALLUXIO CS WITH
      return Objects.equal(mScheme, that.mScheme)
          && Objects.equal(mAuthority, that.mAuthority)
          && Objects.equal(mProperties, that.mProperties)
          && Objects.equal(mOwner, that.mOwner)
          && Objects.equal(mGroup, that.mGroup);
      // ALLUXIO CS END
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("authority", mAuthority)
          .add("scheme", mScheme)
          .add("properties", mProperties)
          // ALLUXIO CS ADD
          .add("owner", mOwner)
          .add("group", mGroup)
          // ALLUXIO CS END
          .toString();
    }
  }

  // TODO(binfan): Add refcount to the UFS instance. Once the refcount goes to zero,
  // we could close this UFS instance.
  /**
   * Maps from key to {@link UnderFileSystem} instances. This map keeps the entire set of UFS
   * instances, each keyed by their unique combination of Uri and conf information. This map
   * helps efficiently identify if a UFS instance in request should be created or can be reused.
   */
  private final ConcurrentHashMap<Key, UnderFileSystem> mUnderFileSystemMap =
      new ConcurrentHashMap<>();
  /**
   * Maps from mount id to {@link UnderFileSystem} instances. This map helps efficiently retrieve
   * an existing UFS instance given its mount id.
   */
  private final ConcurrentHashMap<Long, UnderFileSystem> mMountIdToUnderFileSystemMap =
      new ConcurrentHashMap<>();

  private UnderFileSystem mRootUfs;
  protected final Closer mCloser;

  AbstractUfsManager() {
    mCloser = Closer.create();
  }

  /**
   * Establishes the connection to the given UFS from the server.
   *
   * @param ufs UFS instance
   * @throws IOException if failed to create the UFS instance
   */
  protected abstract void connect(UnderFileSystem ufs) throws IOException;

  /**
   * Return a UFS instance if it already exists in the cache, otherwise, creates a new instance and
   * return this.
   *
   * @param ufsUri the UFS path
   * @param ufsConf the UFS configuration
   * @return the UFS instance
   * @throws IOException if it is failed to create the UFS instance
   */
  private UnderFileSystem getOrAdd(String ufsUri, UnderFileSystemConfiguration ufsConf)
      throws IOException {
    Key key = new Key(new AlluxioURI(ufsUri), ufsConf.getUserSpecifiedConf());
    UnderFileSystem cachedFs = mUnderFileSystemMap.get(key);
    if (cachedFs != null) {
      return cachedFs;
    }
    UnderFileSystem fs = UnderFileSystem.Factory.create(ufsUri, ufsConf);
    try {
      connect(fs);
    } catch (IOException e) {
      fs.close();
      throw e;
    }
    cachedFs = mUnderFileSystemMap.putIfAbsent(key, fs);
    if (cachedFs == null) {
      // above insert is successful
      mCloser.register(fs);
      return fs;
    }
    try {
      fs.close();
    } catch (IOException e) {
      // Cannot close the created ufs which fails the race.
      LOG.error("Failed to close UFS {}", fs, e);
    }
    return cachedFs;
  }

  @Override
  public UnderFileSystem addMount(long mountId, String ufsUri, UnderFileSystemConfiguration ufsConf)
      throws IOException {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    Preconditions.checkArgument(ufsUri != null, "uri");
    Preconditions.checkArgument(ufsConf != null, "ufsConf");
    UnderFileSystem ufs = getOrAdd(ufsUri, ufsConf);
    mMountIdToUnderFileSystemMap.put(mountId, ufs);
    return ufs;
  }

  @Override
  public void removeMount(long mountId) {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    // TODO(binfan): check the refcount of this ufs in mUnderFileSystemMap and remove it if this is
    // no more used. Currently, it is possibly used by out mount too.
    mMountIdToUnderFileSystemMap.remove(mountId);
  }

  @Override
  public UnderFileSystem get(long mountId) {
    return mMountIdToUnderFileSystemMap.get(mountId);
  }

  @Override
  public UnderFileSystem getRoot() {
    synchronized (this) {
      if (mRootUfs == null) {
        String rootUri = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
        boolean rootReadOnly =
            Configuration.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY);
        boolean rootShared = Configuration.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_SHARED);
        Map<String, String> rootConf =
            Configuration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
        try {
          mRootUfs = addMount(IdUtils.ROOT_MOUNT_ID, rootUri,
              new UnderFileSystemConfiguration(rootReadOnly, rootShared, rootConf));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return mRootUfs;
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
}
