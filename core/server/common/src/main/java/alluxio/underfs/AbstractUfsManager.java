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
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.util.IdUtils;

import com.google.common.base.MoreObjects;
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

  private final Object mLock = new Object();

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
      mAuthority = uri.getAuthority().toString().toLowerCase();
      mProperties = (properties == null || properties.isEmpty()) ? null : properties;
      // ALLUXIO CS ADD
      if (alluxio.util.CommonUtils.isAlluxioServer()) {
        mOwner = alluxio.util.SecurityUtils.getOwnerFromGrpcClient();
        mGroup = alluxio.util.SecurityUtils.getGroupFromGrpcClient();
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
      return MoreObjects.toStringHelper(this)
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
  protected final ConcurrentHashMap<Key, UnderFileSystem> mUnderFileSystemMap =
      new ConcurrentHashMap<>();
  /**
   * Maps from mount id to {@link UfsClient} instances. This map helps efficiently retrieve
   * existing UFS info given its mount id.
   */
  private final ConcurrentHashMap<Long, UfsClient> mMountIdToUfsInfoMap =
      new ConcurrentHashMap<>();

  private UfsClient mRootUfsClient;
  protected final Closer mCloser;
  // ALLUXIO CS ADD
  private final Map<Class<? extends UfsService>, UfsServiceFactory> mUfsServicesFactories =
      new ConcurrentHashMap<>();

  private final Map<Long, Map<Class<? extends UfsService>, UfsService>>
      mMountIdToUfsServicesMap = new ConcurrentHashMap<>();
  // ALLUXIO CS END

  AbstractUfsManager() {
    mCloser = Closer.create();
  }

  /**
   * Return a UFS instance if it already exists in the cache, otherwise, creates a new instance and
   * return this.
   *
   * @param ufsUri the UFS path
   * @param ufsConf the UFS configuration
   * @return the UFS instance
   */
  private UnderFileSystem getOrAdd(AlluxioURI ufsUri, UnderFileSystemConfiguration ufsConf) {
    Key key = new Key(ufsUri, ufsConf.getMountSpecificConf());
    UnderFileSystem cachedFs = mUnderFileSystemMap.get(key);
    if (cachedFs != null) {
      return cachedFs;
    }
    // On cache miss, synchronize the creation to ensure ufs is only created once
    synchronized (mLock) {
      cachedFs = mUnderFileSystemMap.get(key);
      if (cachedFs != null) {
        return cachedFs;
      }
      UnderFileSystem fs = UnderFileSystem.Factory.create(ufsUri.toString(), ufsConf);
      mUnderFileSystemMap.putIfAbsent(key, fs);
      mCloser.register(fs);
      return fs;
    }
  }

  @Override
  public void addMount(long mountId, final AlluxioURI ufsUri,
      final UnderFileSystemConfiguration ufsConf) {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    Preconditions.checkNotNull(ufsUri, "ufsUri");
    Preconditions.checkNotNull(ufsConf, "ufsConf");
    mMountIdToUfsInfoMap.put(mountId, new UfsClient(() -> getOrAdd(ufsUri, ufsConf), ufsUri));
    // ALLUXIO CS ADD
    Map<Class<? extends UfsService>, UfsService> ufsServices =
        mMountIdToUfsServicesMap.computeIfAbsent(mountId, id -> new ConcurrentHashMap<>());
    for (Map.Entry<Class<? extends UfsService>, UfsServiceFactory> entry
        : mUfsServicesFactories.entrySet()) {
      ufsServices.computeIfAbsent(entry.getKey(), serviceType -> {
        LOG.debug("Creating UFS service {} for URI {}", serviceType.getName(), ufsUri);
        UfsService service = entry.getValue().createUfsService(ufsUri.toString(),
            ufsConf, serviceType);
        if (service != null) {
          LOG.debug("Registering UFS service {} for URI {}", serviceType.getName(), ufsUri);
        }
        return service;
      });
    }
    // ALLUXIO CS END
  }

  @Override
  public void removeMount(long mountId) {
    Preconditions.checkArgument(mountId != IdUtils.INVALID_MOUNT_ID, "mountId");
    // TODO(binfan): check the refcount of this ufs in mUnderFileSystemMap and remove it if this is
    // no more used. Currently, it is possibly used by out mount too.
    mMountIdToUfsInfoMap.remove(mountId);
    // ALLUXIO CS ADD
    Map<Class<? extends UfsService>, UfsService> ufsServices =
        mMountIdToUfsServicesMap.remove(mountId);
    if (ufsServices != null) {
      for (Map.Entry<Class<? extends UfsService>, UfsService>  entry
          : ufsServices.entrySet()) {
        LOG.debug("Stopping UFS service {} for mount {}", entry.getKey().getName(), mountId);
        try {
          entry.getValue().close();
        } catch (IOException e) {
          LOG.error("Unable to stop service {} for mount {}: {}", entry.getKey().getName(), mountId,
              e.getMessage());
        }
      }
      LOG.debug("Removed UFS services for mount {}", mountId);
    }
    // ALLUXIO CS END
  }

  @Override
  public UfsClient get(long mountId) throws NotFoundException, UnavailableException {
    UfsClient ufsClient = mMountIdToUfsInfoMap.get(mountId);
    if (ufsClient == null) {
      throw new NotFoundException(
          String.format("Mount Id %d not found in cached mount points", mountId));
    }
    return ufsClient;
  }

  @Override
  public UfsClient getRoot() {
    synchronized (this) {
      if (mRootUfsClient == null) {
        String rootUri = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
        boolean rootReadOnly =
            ServerConfiguration.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY);
        boolean rootShared = ServerConfiguration
            .getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_SHARED);
        Map<String, String> rootConf =
            ServerConfiguration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
        addMount(IdUtils.ROOT_MOUNT_ID, new AlluxioURI(rootUri),
            UnderFileSystemConfiguration.defaults().setReadOnly(rootReadOnly).setShared(rootShared)
                .createMountSpecificConf(rootConf));
        try {
          mRootUfsClient = get(IdUtils.ROOT_MOUNT_ID);
        } catch (NotFoundException | UnavailableException e) {
          throw new RuntimeException("We should never reach here", e);
        }
      }
      return mRootUfsClient;
    }
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }
  // ALLUXIO CS ADD

  @Override
  public void registerUfsServiceFactory(Class<? extends UfsService> serviceType,
      UfsServiceFactory factory) {
    Preconditions.checkNotNull(serviceType, "serviceType");
    Preconditions.checkNotNull(factory, "factory");
    mUfsServicesFactories.put(serviceType, factory);
  }

  @Override
  public <T extends UfsService> T getUfsService(long mountId, Class<T> serviceType) {
    Map<Class<? extends UfsService>, UfsService> services =
        mMountIdToUfsServicesMap.get(mountId);
    if (services != null) {
      return (T) services.get(serviceType);
    }
    return null;
  }
  // ALLUXIO CS END
}
