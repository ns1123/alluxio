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

package alluxio.client.file.options;

import alluxio.client.file.URIStatus;
import alluxio.master.block.BlockId;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.BlockInfo;
import alluxio.wire.FileBlockInfo;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information for reading a file. This is an internal options class which contains information
 * from {@link OpenFileOptions} as well as the {@link URIStatus} being read. In addition to
 * providing access to the fields, it provides convenience functions for various nested
 * fields and creating {@link alluxio.proto.dataserver.Protocol.ReadRequest}s.
 */
@NotThreadSafe
// TODO(calvin): Rename this class
public final class InStreamOptions {
<<<<<<< HEAD
  private FileWriteLocationPolicy mCacheLocationPolicy;
  private ReadType mReadType;
  /** Cache incomplete blocks if Alluxio is configured to store blocks in Alluxio storage. */
  private boolean mCachePartiallyReadBlock;
  /**
   * The cache read buffer size in seek. This is only used if {@link #mCachePartiallyReadBlock}
   * is enabled.
   */
  private long mSeekBufferSizeBytes;
  /** The maximum UFS read concurrency for one block on one Alluxio worker. */
  private int mMaxUfsReadConcurrency;
  /** The location policy to determine the worker location to serve UFS block reads. */
  private BlockLocationPolicy mUfsReadLocationPolicy;
  // ALLUXIO CS ADD
  private alluxio.client.security.CapabilityFetcher mCapabilityFetcher = null;
  private boolean mEncrypted = false;
  private alluxio.proto.security.EncryptionProto.Meta mEncryptionMeta = null;
  // ALLUXIO CS END

  /**
   * @return the default {@link InStreamOptions}
   */
  public static InStreamOptions defaults() {
    return new InStreamOptions();
  }

  private InStreamOptions() {
    mReadType = Configuration.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
    mCacheLocationPolicy =
        CommonUtils.createNewClassInstance(Configuration.<FileWriteLocationPolicy>getClass(
            PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    CreateOptions blockLocationPolicyCreateOptions = CreateOptions.defaults()
        .setLocationPolicyClassName(
            Configuration.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY))
        .setDeterministicHashPolicyNumShards(Configuration
            .getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS));
    mUfsReadLocationPolicy = BlockLocationPolicy.Factory.create(blockLocationPolicyCreateOptions);
    mCachePartiallyReadBlock =
        Configuration.getBoolean(PropertyKey.USER_FILE_CACHE_PARTIALLY_READ_BLOCK);
    mSeekBufferSizeBytes = Configuration.getBytes(PropertyKey.USER_FILE_SEEK_BUFFER_SIZE_BYTES);
    mMaxUfsReadConcurrency =
        Configuration.getInt(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX);
  }

  /**
   * @return the location policy to use when storing data to Alluxio
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link InStreamOptions#getCacheLocationPolicy()}.
   */
  @Deprecated
  public FileWriteLocationPolicy getLocationPolicy() {
    return mCacheLocationPolicy;
  }

  /**
   * @return the location policy to use when storing data to Alluxio
   */
  public FileWriteLocationPolicy getCacheLocationPolicy() {
    return mCacheLocationPolicy;
  }

  /**
   * @return the Alluxio storage type
   */
  public AlluxioStorageType getAlluxioStorageType() {
    return mReadType.getAlluxioStorageType();
  }

  /**
   * @return the maximum UFS read concurrency
   */
  public int getMaxUfsReadConcurrency() {
    return mMaxUfsReadConcurrency;
  }

  /**
   * @return the UFS read location policy
   */
  public BlockLocationPolicy getUfsReadLocationPolicy() {
    return mUfsReadLocationPolicy;
  }

  /**
   * @param policy the location policy to use when storing data to Alluxio
   * @return the updated options object
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link InStreamOptions#setCacheLocationPolicy(FileWriteLocationPolicy)}.
   */
  @Deprecated
  public InStreamOptions setLocationPolicy(FileWriteLocationPolicy policy) {
    mCacheLocationPolicy = policy;
    return this;
  }

  /**
   * @param policy the location policy to use when storing data to Alluxio
   * @return the updated options object
   */
  public InStreamOptions setCacheLocationPolicy(FileWriteLocationPolicy policy) {
    mCacheLocationPolicy = policy;
    return this;
  }

  /**
   * Sets the {@link ReadType}.
   *
   * @param readType the {@link ReadType} for this operation. Setting this will override the
   *        {@link AlluxioStorageType}.
   * @return the updated options object
   */
  public InStreamOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }
||||||| merged common ancestors
  private FileWriteLocationPolicy mCacheLocationPolicy;
  private ReadType mReadType;
  /** Cache incomplete blocks if Alluxio is configured to store blocks in Alluxio storage. */
  private boolean mCachePartiallyReadBlock;
  /**
   * The cache read buffer size in seek. This is only used if {@link #mCachePartiallyReadBlock}
   * is enabled.
   */
  private long mSeekBufferSizeBytes;
  /** The maximum UFS read concurrency for one block on one Alluxio worker. */
  private int mMaxUfsReadConcurrency;
  /** The location policy to determine the worker location to serve UFS block reads. */
  private BlockLocationPolicy mUfsReadLocationPolicy;

  /**
   * @return the default {@link InStreamOptions}
   */
  public static InStreamOptions defaults() {
    return new InStreamOptions();
  }

  private InStreamOptions() {
    mReadType = Configuration.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
    mCacheLocationPolicy =
        CommonUtils.createNewClassInstance(Configuration.<FileWriteLocationPolicy>getClass(
            PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    CreateOptions blockLocationPolicyCreateOptions = CreateOptions.defaults()
        .setLocationPolicyClassName(
            Configuration.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY))
        .setDeterministicHashPolicyNumShards(Configuration
            .getInt(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY_DETERMINISTIC_HASH_SHARDS));
    mUfsReadLocationPolicy = BlockLocationPolicy.Factory.create(blockLocationPolicyCreateOptions);
    mCachePartiallyReadBlock =
        Configuration.getBoolean(PropertyKey.USER_FILE_CACHE_PARTIALLY_READ_BLOCK);
    mSeekBufferSizeBytes = Configuration.getBytes(PropertyKey.USER_FILE_SEEK_BUFFER_SIZE_BYTES);
    mMaxUfsReadConcurrency =
        Configuration.getInt(PropertyKey.USER_UFS_BLOCK_READ_CONCURRENCY_MAX);
  }

  /**
   * @return the location policy to use when storing data to Alluxio
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link InStreamOptions#getCacheLocationPolicy()}.
   */
  @Deprecated
  public FileWriteLocationPolicy getLocationPolicy() {
    return mCacheLocationPolicy;
  }

  /**
   * @return the location policy to use when storing data to Alluxio
   */
  public FileWriteLocationPolicy getCacheLocationPolicy() {
    return mCacheLocationPolicy;
  }

  /**
   * @return the Alluxio storage type
   */
  public AlluxioStorageType getAlluxioStorageType() {
    return mReadType.getAlluxioStorageType();
  }

  /**
   * @return the maximum UFS read concurrency
   */
  public int getMaxUfsReadConcurrency() {
    return mMaxUfsReadConcurrency;
  }

  /**
   * @return the UFS read location policy
   */
  public BlockLocationPolicy getUfsReadLocationPolicy() {
    return mUfsReadLocationPolicy;
  }

  /**
   * @param policy the location policy to use when storing data to Alluxio
   * @return the updated options object
   * @deprecated since version 1.5 and will be removed in version 2.0. Use
   *             {@link InStreamOptions#setCacheLocationPolicy(FileWriteLocationPolicy)}.
   */
  @Deprecated
  public InStreamOptions setLocationPolicy(FileWriteLocationPolicy policy) {
    mCacheLocationPolicy = policy;
    return this;
  }

  /**
   * @param policy the location policy to use when storing data to Alluxio
   * @return the updated options object
   */
  public InStreamOptions setCacheLocationPolicy(FileWriteLocationPolicy policy) {
    mCacheLocationPolicy = policy;
    return this;
  }

  /**
   * Sets the {@link ReadType}.
   *
   * @param readType the {@link ReadType} for this operation. Setting this will override the
   *        {@link AlluxioStorageType}.
   * @return the updated options object
   */
  public InStreamOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }
=======
  private final URIStatus mStatus;
  private final OpenFileOptions mOptions;
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded

  /**
   * Creates with the default {@link OpenFileOptions}.
   * @param status the file to create the options for
   */
  public InStreamOptions(URIStatus status) {
    this(status, OpenFileOptions.defaults());
  }

  /**
   * Creates based on the arguments provided.
   * @param status the file to create the options for
   * @param options the {@link OpenFileOptions} to use
   */
  public InStreamOptions(URIStatus status, OpenFileOptions options) {
    mStatus = status;
    mOptions = options;
  }

  /**
   * @return the {@link OpenFileOptions} associated with the instream
   */
  public OpenFileOptions getOptions() {
    return mOptions;
  }

  /**
   * @return the {@link URIStatus} associated with the instream
   */
  public URIStatus getStatus() {
    return mStatus;
  }

  /**
   * @param blockId id of the block
   * @return the block info associated with the block id
   */
  public BlockInfo getBlockInfo(long blockId) {
    Preconditions.checkArgument(mStatus.getBlockIds().contains(blockId), "blockId");
    return mStatus.getFileBlockInfos().stream().map(FileBlockInfo::getBlockInfo)
        .filter(blockInfo -> blockInfo.getBlockId() == blockId).findFirst().get();
  }
  // ALLUXIO CS ADD

  /**
   * @return the capability getter
   */
  public alluxio.client.security.CapabilityFetcher getCapabilityFetcher() {
    return mCapabilityFetcher;
  }

  /**
   * @return whether the file is encrypted or not
   */
  public boolean isEncrypted() {
    return mEncrypted;
  }

  /**
   * @return the encryption meta
   */
  public alluxio.proto.security.EncryptionProto.Meta getEncryptionMeta() {
    return mEncryptionMeta;
  }

  /**
   * @param fetcher the capability fetcher to set
   * @return the updated object
   */
  public InStreamOptions setCapabilityFetcher(alluxio.client.security.CapabilityFetcher fetcher) {
    mCapabilityFetcher = fetcher;
    return this;
  }

  /**
   * @param encrypted the encrypted flag value to use
   * @return the updated object
   */
  public InStreamOptions setEncrypted(boolean encrypted) {
    mEncrypted = encrypted;
    return this;
  }

  /**
   * @param meta the encryption metadata
   * @return the updated object
   */
  public InStreamOptions setEncryptionMeta(alluxio.proto.security.EncryptionProto.Meta meta) {
    mEncryptionMeta = meta;
    return this;
  }
  // ALLUXIO CS END

  /**
   * @param blockId id of the block
   * @return a {@link Protocol.OpenUfsBlockOptions} based on the block id and options
   */
  public Protocol.OpenUfsBlockOptions getOpenUfsBlockOptions(long blockId) {
    Preconditions.checkArgument(mStatus.getBlockIds().contains(blockId), "blockId");
    long blockStart = BlockId.getSequenceNumber(blockId) * mStatus.getBlockSizeBytes();
    BlockInfo info = getBlockInfo(blockId);
    return Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(mStatus.getUfsPath())
        .setOffsetInFile(blockStart).setBlockSize(info.getLength())
        .setMaxUfsReadConcurrency(mOptions.getMaxUfsReadConcurrency())
        .setNoCache(!mOptions.getReadType().isCache()).setMountId(mStatus.getMountId()).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InStreamOptions)) {
      return false;
    }
    InStreamOptions that = (InStreamOptions) o;
<<<<<<< HEAD
    return Objects.equal(mCacheLocationPolicy, that.mCacheLocationPolicy)
        && Objects.equal(mReadType, that.mReadType)
        && Objects.equal(mCachePartiallyReadBlock, that.mCachePartiallyReadBlock)
        // ALLUXIO CS ADD
        && Objects.equal(mCapabilityFetcher, that.mCapabilityFetcher)
        && Objects.equal(mEncrypted, that.mEncrypted)
        && Objects.equal(mEncryptionMeta, that.mEncryptionMeta)
        // ALLUXIO CS END
        && Objects.equal(mSeekBufferSizeBytes, that.mSeekBufferSizeBytes)
        && Objects.equal(mMaxUfsReadConcurrency, that.mMaxUfsReadConcurrency)
        && Objects.equal(mUfsReadLocationPolicy, that.mUfsReadLocationPolicy);
||||||| merged common ancestors
    return Objects.equal(mCacheLocationPolicy, that.mCacheLocationPolicy)
        && Objects.equal(mReadType, that.mReadType)
        && Objects.equal(mCachePartiallyReadBlock, that.mCachePartiallyReadBlock)
        && Objects.equal(mSeekBufferSizeBytes, that.mSeekBufferSizeBytes)
        && Objects.equal(mMaxUfsReadConcurrency, that.mMaxUfsReadConcurrency)
        && Objects.equal(mUfsReadLocationPolicy, that.mUfsReadLocationPolicy);
=======
    return Objects.equal(mStatus, that.mStatus) && Objects.equal(mOptions, that.mOptions);
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded
  }

  @Override
  public int hashCode() {
<<<<<<< HEAD
    return Objects
        .hashCode(
            mCacheLocationPolicy,
            mReadType,
            mCachePartiallyReadBlock,
            // ALLUXIO CS ADD
            mCapabilityFetcher,
            mEncrypted,
            mEncryptionMeta,
            // ALLUXIO CS END
            mSeekBufferSizeBytes,
            mMaxUfsReadConcurrency,
            mUfsReadLocationPolicy);
||||||| merged common ancestors
    return Objects
        .hashCode(
            mCacheLocationPolicy,
            mReadType,
            mCachePartiallyReadBlock,
            mSeekBufferSizeBytes,
            mMaxUfsReadConcurrency,
            mUfsReadLocationPolicy);
=======
    return Objects.hashCode(mStatus, mOptions);
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded
  }

  @Override
  public String toString() {
<<<<<<< HEAD
    return Objects.toStringHelper(this).add("cacheLocationPolicy", mCacheLocationPolicy)
        .add("readType", mReadType).add("cachePartiallyReadBlock", mCachePartiallyReadBlock)
        // ALLUXIO CS ADD
        .add("capabilityFetcher", mCapabilityFetcher)
        .add("encrypted", mEncrypted)
        .add("encryptionMeta", mEncryptionMeta)
        // ALLUXIO CS END
        .add("seekBufferSize", mSeekBufferSizeBytes)
        .add("maxUfsReadConcurrency", mMaxUfsReadConcurrency)
        .add("ufsReadLocationPolicy", mUfsReadLocationPolicy).toString();
||||||| merged common ancestors
    return Objects.toStringHelper(this).add("cacheLocationPolicy", mCacheLocationPolicy)
        .add("readType", mReadType).add("cachePartiallyReadBlock", mCachePartiallyReadBlock)
        .add("seekBufferSize", mSeekBufferSizeBytes)
        .add("maxUfsReadConcurrency", mMaxUfsReadConcurrency)
        .add("ufsReadLocationPolicy", mUfsReadLocationPolicy).toString();
=======
    return Objects.toStringHelper(this).add("URIStatus", mStatus).add("OpenFileOptions", mOptions)
        .toString();
>>>>>>> 1a2e8078327a0651716e3313a4a085de4ff40ded
  }
}
