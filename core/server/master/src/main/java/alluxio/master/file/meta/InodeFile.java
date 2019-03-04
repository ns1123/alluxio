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

package alluxio.master.file.meta;

import alluxio.exception.BlockInfoException;

import java.util.List;

/**
 * Forwarding wrapper around an inode file view.
 */
<<<<<<< HEAD
@NotThreadSafe
public final class InodeFile extends Inode<InodeFile> implements InodeFileView {
  private List<Long> mBlocks;
  private long mBlockContainerId;
  private long mBlockSizeBytes;
  private boolean mCacheable;
  private boolean mCompleted;
  private long mLength;
  private long mPersistJobId;
  private int mReplicationDurable;
  private int mReplicationMax;
  private int mReplicationMin;
  private String mTempUfsPath;
  // ALLUXIO CS ADD
  private boolean mEncrypted;
  // ALLUXIO CS END
||||||| merged common ancestors
@NotThreadSafe
public final class InodeFile extends Inode<InodeFile> implements InodeFileView {
  private List<Long> mBlocks;
  private long mBlockContainerId;
  private long mBlockSizeBytes;
  private boolean mCacheable;
  private boolean mCompleted;
  private long mLength;
  private long mPersistJobId;
  private int mReplicationDurable;
  private int mReplicationMax;
  private int mReplicationMin;
  private String mTempUfsPath;
=======
public class InodeFile extends Inode implements InodeFileView {
  private final InodeFileView mDelegate;
>>>>>>> upstream-os/master

  /**
   * @param delegate the inode to delegate to
   */
<<<<<<< HEAD
  private InodeFile(long blockContainerId) {
    super(BlockId.createBlockId(blockContainerId, BlockId.getMaxSequenceNumber()), false);
    mBlocks = new ArrayList<>(1);
    mBlockContainerId = blockContainerId;
    mBlockSizeBytes = 0;
    mCacheable = false;
    mCompleted = false;
    mLength = 0;
    mPersistJobId = Constants.PERSISTENCE_INVALID_JOB_ID;
    mReplicationDurable = 0;
    mReplicationMax = Constants.REPLICATION_MAX_INFINITY;
    mReplicationMin = 0;
    mTempUfsPath = Constants.PERSISTENCE_INVALID_UFS_PATH;
    // ALLUXIO CS ADD
    mEncrypted = false;
    // ALLUXIO CS END
||||||| merged common ancestors
  private InodeFile(long blockContainerId) {
    super(BlockId.createBlockId(blockContainerId, BlockId.getMaxSequenceNumber()), false);
    mBlocks = new ArrayList<>(1);
    mBlockContainerId = blockContainerId;
    mBlockSizeBytes = 0;
    mCacheable = false;
    mCompleted = false;
    mLength = 0;
    mPersistJobId = Constants.PERSISTENCE_INVALID_JOB_ID;
    mReplicationDurable = 0;
    mReplicationMax = Constants.REPLICATION_MAX_INFINITY;
    mReplicationMin = 0;
    mTempUfsPath = Constants.PERSISTENCE_INVALID_UFS_PATH;
=======
  public InodeFile(InodeFileView delegate) {
    super(delegate);
    mDelegate = delegate;
>>>>>>> upstream-os/master
  }

  @Override
  public long getPersistJobId() {
    return mDelegate.getPersistJobId();
  }

  @Override
<<<<<<< HEAD
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();
    // note: in-Alluxio percentage is NOT calculated here, because it needs blocks info stored in
    // block master
    ret.setFileId(getId());
    ret.setName(getName());
    ret.setPath(path);
    ret.setLength(getLength());
    ret.setBlockSizeBytes(getBlockSizeBytes());
    ret.setCreationTimeMs(getCreationTimeMs());
    ret.setCacheable(isCacheable());
    ret.setFolder(isDirectory());
    ret.setPinned(isPinned());
    ret.setCompleted(isCompleted());
    ret.setPersisted(isPersisted());
    ret.setBlockIds(getBlockIds());
    ret.setLastModificationTimeMs(getLastModificationTimeMs());
    ret.setTtl(mTtl);
    ret.setTtlAction(mTtlAction);
    ret.setOwner(getOwner());
    ret.setGroup(getGroup());
    ret.setMode(getMode());
    ret.setPersistenceState(getPersistenceState().toString());
    ret.setMountPoint(false);
    ret.setReplicationMax(getReplicationMax());
    ret.setReplicationMin(getReplicationMin());
    // ALLUXIO CS ADD
    ret.setEncrypted(isEncrypted());
    // ALLUXIO CS END
    ret.setUfsFingerprint(getUfsFingerprint());
    ret.setAcl(mAcl);
    return ret;
||||||| merged common ancestors
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();
    // note: in-Alluxio percentage is NOT calculated here, because it needs blocks info stored in
    // block master
    ret.setFileId(getId());
    ret.setName(getName());
    ret.setPath(path);
    ret.setLength(getLength());
    ret.setBlockSizeBytes(getBlockSizeBytes());
    ret.setCreationTimeMs(getCreationTimeMs());
    ret.setCacheable(isCacheable());
    ret.setFolder(isDirectory());
    ret.setPinned(isPinned());
    ret.setCompleted(isCompleted());
    ret.setPersisted(isPersisted());
    ret.setBlockIds(getBlockIds());
    ret.setLastModificationTimeMs(getLastModificationTimeMs());
    ret.setTtl(mTtl);
    ret.setTtlAction(mTtlAction);
    ret.setOwner(getOwner());
    ret.setGroup(getGroup());
    ret.setMode(getMode());
    ret.setPersistenceState(getPersistenceState().toString());
    ret.setMountPoint(false);
    ret.setReplicationMax(getReplicationMax());
    ret.setReplicationMin(getReplicationMin());
    ret.setUfsFingerprint(getUfsFingerprint());
    ret.setAcl(mAcl);
    return ret;
=======
  public int getReplicationDurable() {
    return mDelegate.getReplicationDurable();
>>>>>>> upstream-os/master
  }

  @Override
  public int getReplicationMax() {
    return mDelegate.getReplicationMax();
  }

  @Override
  public int getReplicationMin() {
    return mDelegate.getReplicationMin();
  }

  @Override
  public String getTempUfsPath() {
    return mDelegate.getTempUfsPath();
  }

  @Override
  public List<Long> getBlockIds() {
    return mDelegate.getBlockIds();
  }

  @Override
  public long getBlockSizeBytes() {
    return mDelegate.getBlockSizeBytes();
  }

  @Override
  public long getLength() {
    return mDelegate.getLength();
  }

  @Override
  public long getBlockContainerId() {
    return mDelegate.getBlockContainerId();
  }

  @Override
  public long getBlockIdByIndex(int blockIndex) throws BlockInfoException {
    return mDelegate.getBlockIdByIndex(blockIndex);
  }

  // ALLUXIO CS ADD
  @Override
  public boolean isEncrypted() {
    return mEncrypted;
  }

  // ALLUXIO CS END
  @Override
  public boolean isCacheable() {
    return mDelegate.isCacheable();
  }

  @Override
  public boolean isCompleted() {
<<<<<<< HEAD
    return mCompleted;
  }

  /**
   * @return the id of a new block of the file
   */
  public long getNewBlockId() {
    long blockId = BlockId.createBlockId(mBlockContainerId, mBlocks.size());
    // TODO(gene): Check for max block sequence number, and sanity check the sequence number.
    // TODO(gene): Check isComplete?
    mBlocks.add(blockId);
    return blockId;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated object
   */
  public InodeFile setBlockSizeBytes(long blockSizeBytes) {
    Preconditions.checkArgument(blockSizeBytes >= 0, "Block size cannot be negative");
    mBlockSizeBytes = blockSizeBytes;
    return getThis();
  }

  /**
   * @param blockIds the id's of the block
   * @return the updated object
   */
  public InodeFile setBlockIds(List<Long> blockIds) {
    mBlocks = new ArrayList<>(Preconditions.checkNotNull(blockIds, "blockIds"));
    return getThis();
  }

  /**
   * @param cacheable the cacheable flag value to use
   * @return the updated object
   */
  public InodeFile setCacheable(boolean cacheable) {
    // TODO(gene). This related logic is not complete right. Fix this.
    mCacheable = cacheable;
    return getThis();
  }

  /**
   * @param completed the complete flag value to use
   * @return the updated object
   */
  public InodeFile setCompleted(boolean completed) {
    mCompleted = completed;
    return getThis();
  }

  /**
   * @param length the length to use
   * @return the updated object
   */
  public InodeFile setLength(long length) {
    mLength = length;
    return getThis();
  }

  /**
   * @param persistJobId the id of the job persisting this file
   * @return the updated object
   */
  public InodeFile setPersistJobId(long persistJobId) {
    mPersistJobId = persistJobId;
    return getThis();
  }

  /**
   * @param replicationDurable the durable number of block replication
   * @return the updated object
   */
  public InodeFile setReplicationDurable(int replicationDurable) {
    mReplicationDurable = replicationDurable;
    return getThis();
  }

  /**
   * @param replicationMax the maximum number of block replication
   * @return the updated object
   */
  public InodeFile setReplicationMax(int replicationMax) {
    mReplicationMax = replicationMax;
    return getThis();
  }

  /**
   * @param replicationMin the minimum number of block replication
   * @return the updated object
   */
  public InodeFile setReplicationMin(int replicationMin) {
    mReplicationMin = replicationMin;
    return getThis();
  }

  /**
   * @param tempUfsPath the temporary UFS path this file is persisted to
   * @return the updated object
   */
  public InodeFile setTempUfsPath(String tempUfsPath) {
    mTempUfsPath = tempUfsPath;
    return getThis();
  }

  // ALLUXIO CS ADD
  /**
   * @param encrypted the encrypted flag value to use
   * @return the updated object
   */
  public InodeFile setEncrypted(boolean encrypted) {
    mEncrypted = encrypted;
    return getThis();
  }

  // ALLUXIO CS END
  /**
   * Updates this inode file's state from the given entry.
   *
   * @param entry the entry
   */
  public void updateFromEntry(UpdateInodeFileEntry entry) {
    if (entry.hasPersistJobId()) {
      setPersistJobId(entry.getPersistJobId());
    }
    if (entry.hasReplicationMax()) {
      setReplicationMax(entry.getReplicationMax());
    }
    if (entry.hasReplicationMin()) {
      setReplicationMin(entry.getReplicationMin());
    }
    if (entry.hasTempUfsPath()) {
      setTempUfsPath(entry.getTempUfsPath());
    }
    if (entry.hasBlockSizeBytes()) {
      setBlockSizeBytes(entry.getBlockSizeBytes());
    }
    if (entry.hasCacheable()) {
      setCacheable(entry.getCacheable());
    }
    if (entry.hasCompleted()) {
      setCompleted(entry.getCompleted());
    }
    if (entry.hasLength()) {
      setLength(entry.getLength());
    }
    if (entry.getSetBlocksCount() > 0) {
      setBlockIds(entry.getSetBlocksList());
    }
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("blocks", mBlocks)
        .add("blockContainerId", mBlockContainerId)
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("cacheable", mCacheable)
        .add("completed", mCompleted)
        .add("persistJobId", mPersistJobId)
        .add("replicationDurable", mReplicationDurable)
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        .add("tempUfsPath", mTempUfsPath)
        // ALLUXIO CS ADD
        .add("encrypted", mEncrypted)
        // ALLUXIO CS END
        .add("length", mLength).toString();
  }

  /**
   * Converts the entry to an {@link InodeFile}.
   *
   * @param entry the entry to convert
   * @return the {@link InodeFile} representation
   */
  public static InodeFile fromJournalEntry(InodeFileEntry entry) {
    // If journal entry has no mode set, set default mode for backwards-compatibility.
    InodeFile ret = new InodeFile(BlockId.getContainerId(entry.getId()))
        .setName(entry.getName())
        .setBlockIds(entry.getBlocksList())
        .setBlockSizeBytes(entry.getBlockSizeBytes())
        .setCacheable(entry.getCacheable())
        .setCompleted(entry.getCompleted())
        .setCreationTimeMs(entry.getCreationTimeMs())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs(), true)
        .setLength(entry.getLength())
        .setParentId(entry.getParentId())
        .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
        .setPinned(entry.getPinned())
        .setPersistJobId(entry.getPersistJobId())
        .setReplicationDurable(entry.getReplicationDurable())
        .setReplicationMax(entry.getReplicationMax())
        .setReplicationMin(entry.getReplicationMin())
        .setTempUfsPath(entry.getTempUfsPath())
        // ALLUXIO CS ADD
        .setEncrypted(entry.getEncrypted())
        // ALLUXIO CS END
        .setTtl(entry.getTtl())
        .setTtlAction((ProtobufUtils.fromProtobuf(entry.getTtlAction())))
        .setUfsFingerprint(entry.hasUfsFingerprint() ? entry.getUfsFingerprint() :
            Constants.INVALID_UFS_FINGERPRINT);
    if (entry.hasAcl()) {
      ret.mAcl = ProtoUtils.fromProto(entry.getAcl());
    } else {
      // Backward compatibility.
      AccessControlList acl = new AccessControlList();
      acl.setOwningUser(entry.getOwner());
      acl.setOwningGroup(entry.getGroup());
      short mode = entry.hasMode() ? (short) entry.getMode() : Constants.DEFAULT_FILE_SYSTEM_MODE;
      acl.setMode(mode);
      ret.mAcl = acl;
    }
    return ret;
  }

  /**
   * Creates an {@link InodeFile}.
   *
   * @param blockContainerId block container id of this inode
   * @param parentId id of the parent of this inode
   * @param name name of this inode
   * @param creationTimeMs the creation time for this inode
   * @param context context to create this file
   * @return the {@link InodeFile} representation
   */
  public static InodeFile create(long blockContainerId, long parentId, String name,
      long creationTimeMs, CreateFileContext context) {
    Preconditions.checkArgument(
            context.getOptions().getReplicationMax() == Constants.REPLICATION_MAX_INFINITY
        || context.getOptions().getReplicationMax() >= context.getOptions().getReplicationMin());
    return new InodeFile(blockContainerId)
        .setBlockSizeBytes(context.getOptions().getBlockSizeBytes())
        .setCreationTimeMs(creationTimeMs)
        .setName(name)
        .setReplicationDurable(context.getOptions().getReplicationDurable())
        .setReplicationMax(context.getOptions().getReplicationMax())
        .setReplicationMin(context.getOptions().getReplicationMin())
        .setTtl(context.getOptions().getCommonOptions().getTtl())
        .setTtlAction(context.getOptions().getCommonOptions().getTtlAction())
        // ALLUXIO CS ADD
        .setEncrypted(context.isEncrypted())
        // ALLUXIO CS END
        .setParentId(parentId)
        .setLastModificationTimeMs(context.getOperationTimeMs(), true)
        .setOwner(context.getOwner())
        .setGroup(context.getGroup())
        .setMode(context.getMode().toShort())
        .setAcl(context.getAcl())
        .setPersistenceState(context.getPersisted() ? PersistenceState.PERSISTED
            : PersistenceState.NOT_PERSISTED);
  }

  @Override
  public JournalEntry toJournalEntry() {
    InodeFileEntry inodeFile = InodeFileEntry.newBuilder()
        .addAllBlocks(getBlockIds())
        .setBlockSizeBytes(getBlockSizeBytes())
        .setCacheable(isCacheable())
        .setCompleted(isCompleted())
        .setCreationTimeMs(getCreationTimeMs())
        .setId(getId())
        .setLastModificationTimeMs(getLastModificationTimeMs())
        .setLength(getLength())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setPinned(isPinned())
        .setReplicationDurable(getReplicationDurable())
        .setReplicationMax(getReplicationMax())
        .setReplicationMin(getReplicationMin())
        .setPersistJobId(getPersistJobId())
        .setTempUfsPath(getTempUfsPath())
        // ALLUXIO CS ADD
        .setEncrypted(isEncrypted())
        // ALLUXIO CS END
        .setTtl(getTtl())
        .setTtlAction(ProtobufUtils.toProtobuf(getTtlAction()))
        .setUfsFingerprint(getUfsFingerprint())
        .setAcl(ProtoUtils.toProto(mAcl))
        .build();
    return JournalEntry.newBuilder().setInodeFile(inodeFile).build();
||||||| merged common ancestors
    return mCompleted;
  }

  /**
   * @return the id of a new block of the file
   */
  public long getNewBlockId() {
    long blockId = BlockId.createBlockId(mBlockContainerId, mBlocks.size());
    // TODO(gene): Check for max block sequence number, and sanity check the sequence number.
    // TODO(gene): Check isComplete?
    mBlocks.add(blockId);
    return blockId;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated object
   */
  public InodeFile setBlockSizeBytes(long blockSizeBytes) {
    Preconditions.checkArgument(blockSizeBytes >= 0, "Block size cannot be negative");
    mBlockSizeBytes = blockSizeBytes;
    return getThis();
  }

  /**
   * @param blockIds the id's of the block
   * @return the updated object
   */
  public InodeFile setBlockIds(List<Long> blockIds) {
    mBlocks = new ArrayList<>(Preconditions.checkNotNull(blockIds, "blockIds"));
    return getThis();
  }

  /**
   * @param cacheable the cacheable flag value to use
   * @return the updated object
   */
  public InodeFile setCacheable(boolean cacheable) {
    // TODO(gene). This related logic is not complete right. Fix this.
    mCacheable = cacheable;
    return getThis();
  }

  /**
   * @param completed the complete flag value to use
   * @return the updated object
   */
  public InodeFile setCompleted(boolean completed) {
    mCompleted = completed;
    return getThis();
  }

  /**
   * @param length the length to use
   * @return the updated object
   */
  public InodeFile setLength(long length) {
    mLength = length;
    return getThis();
  }

  /**
   * @param persistJobId the id of the job persisting this file
   * @return the updated object
   */
  public InodeFile setPersistJobId(long persistJobId) {
    mPersistJobId = persistJobId;
    return getThis();
  }

  /**
   * @param replicationDurable the durable number of block replication
   * @return the updated object
   */
  public InodeFile setReplicationDurable(int replicationDurable) {
    mReplicationDurable = replicationDurable;
    return getThis();
  }

  /**
   * @param replicationMax the maximum number of block replication
   * @return the updated object
   */
  public InodeFile setReplicationMax(int replicationMax) {
    mReplicationMax = replicationMax;
    return getThis();
  }

  /**
   * @param replicationMin the minimum number of block replication
   * @return the updated object
   */
  public InodeFile setReplicationMin(int replicationMin) {
    mReplicationMin = replicationMin;
    return getThis();
  }

  /**
   * @param tempUfsPath the temporary UFS path this file is persisted to
   * @return the updated object
   */
  public InodeFile setTempUfsPath(String tempUfsPath) {
    mTempUfsPath = tempUfsPath;
    return getThis();
  }

  /**
   * Updates this inode file's state from the given entry.
   *
   * @param entry the entry
   */
  public void updateFromEntry(UpdateInodeFileEntry entry) {
    if (entry.hasPersistJobId()) {
      setPersistJobId(entry.getPersistJobId());
    }
    if (entry.hasReplicationMax()) {
      setReplicationMax(entry.getReplicationMax());
    }
    if (entry.hasReplicationMin()) {
      setReplicationMin(entry.getReplicationMin());
    }
    if (entry.hasTempUfsPath()) {
      setTempUfsPath(entry.getTempUfsPath());
    }
    if (entry.hasBlockSizeBytes()) {
      setBlockSizeBytes(entry.getBlockSizeBytes());
    }
    if (entry.hasCacheable()) {
      setCacheable(entry.getCacheable());
    }
    if (entry.hasCompleted()) {
      setCompleted(entry.getCompleted());
    }
    if (entry.hasLength()) {
      setLength(entry.getLength());
    }
    if (entry.getSetBlocksCount() > 0) {
      setBlockIds(entry.getSetBlocksList());
    }
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("blocks", mBlocks)
        .add("blockContainerId", mBlockContainerId)
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("cacheable", mCacheable)
        .add("completed", mCompleted)
        .add("persistJobId", mPersistJobId)
        .add("replicationDurable", mReplicationDurable)
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        .add("tempUfsPath", mTempUfsPath)
        .add("length", mLength).toString();
  }

  /**
   * Converts the entry to an {@link InodeFile}.
   *
   * @param entry the entry to convert
   * @return the {@link InodeFile} representation
   */
  public static InodeFile fromJournalEntry(InodeFileEntry entry) {
    // If journal entry has no mode set, set default mode for backwards-compatibility.
    InodeFile ret = new InodeFile(BlockId.getContainerId(entry.getId()))
        .setName(entry.getName())
        .setBlockIds(entry.getBlocksList())
        .setBlockSizeBytes(entry.getBlockSizeBytes())
        .setCacheable(entry.getCacheable())
        .setCompleted(entry.getCompleted())
        .setCreationTimeMs(entry.getCreationTimeMs())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs(), true)
        .setLength(entry.getLength())
        .setParentId(entry.getParentId())
        .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
        .setPinned(entry.getPinned())
        .setPersistJobId(entry.getPersistJobId())
        .setReplicationDurable(entry.getReplicationDurable())
        .setReplicationMax(entry.getReplicationMax())
        .setReplicationMin(entry.getReplicationMin())
        .setTempUfsPath(entry.getTempUfsPath())
        .setTtl(entry.getTtl())
        .setTtlAction((ProtobufUtils.fromProtobuf(entry.getTtlAction())))
        .setUfsFingerprint(entry.hasUfsFingerprint() ? entry.getUfsFingerprint() :
            Constants.INVALID_UFS_FINGERPRINT);
    if (entry.hasAcl()) {
      ret.mAcl = ProtoUtils.fromProto(entry.getAcl());
    } else {
      // Backward compatibility.
      AccessControlList acl = new AccessControlList();
      acl.setOwningUser(entry.getOwner());
      acl.setOwningGroup(entry.getGroup());
      short mode = entry.hasMode() ? (short) entry.getMode() : Constants.DEFAULT_FILE_SYSTEM_MODE;
      acl.setMode(mode);
      ret.mAcl = acl;
    }
    return ret;
  }

  /**
   * Creates an {@link InodeFile}.
   *
   * @param blockContainerId block container id of this inode
   * @param parentId id of the parent of this inode
   * @param name name of this inode
   * @param creationTimeMs the creation time for this inode
   * @param context context to create this file
   * @return the {@link InodeFile} representation
   */
  public static InodeFile create(long blockContainerId, long parentId, String name,
      long creationTimeMs, CreateFileContext context) {
    Preconditions.checkArgument(
            context.getOptions().getReplicationMax() == Constants.REPLICATION_MAX_INFINITY
        || context.getOptions().getReplicationMax() >= context.getOptions().getReplicationMin());
    return new InodeFile(blockContainerId)
        .setBlockSizeBytes(context.getOptions().getBlockSizeBytes())
        .setCreationTimeMs(creationTimeMs)
        .setName(name)
        .setReplicationDurable(context.getOptions().getReplicationDurable())
        .setReplicationMax(context.getOptions().getReplicationMax())
        .setReplicationMin(context.getOptions().getReplicationMin())
        .setTtl(context.getOptions().getCommonOptions().getTtl())
        .setTtlAction(context.getOptions().getCommonOptions().getTtlAction())
        .setParentId(parentId)
        .setLastModificationTimeMs(context.getOperationTimeMs(), true)
        .setOwner(context.getOwner())
        .setGroup(context.getGroup())
        .setMode(context.getMode().toShort())
        .setAcl(context.getAcl())
        .setPersistenceState(context.getPersisted() ? PersistenceState.PERSISTED
            : PersistenceState.NOT_PERSISTED);
  }

  @Override
  public JournalEntry toJournalEntry() {
    InodeFileEntry inodeFile = InodeFileEntry.newBuilder()
        .addAllBlocks(getBlockIds())
        .setBlockSizeBytes(getBlockSizeBytes())
        .setCacheable(isCacheable())
        .setCompleted(isCompleted())
        .setCreationTimeMs(getCreationTimeMs())
        .setId(getId())
        .setLastModificationTimeMs(getLastModificationTimeMs())
        .setLength(getLength())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setPinned(isPinned())
        .setReplicationDurable(getReplicationDurable())
        .setReplicationMax(getReplicationMax())
        .setReplicationMin(getReplicationMin())
        .setPersistJobId(getPersistJobId())
        .setTempUfsPath(getTempUfsPath())
        .setTtl(getTtl())
        .setTtlAction(ProtobufUtils.toProtobuf(getTtlAction()))
        .setUfsFingerprint(getUfsFingerprint())
        .setAcl(ProtoUtils.toProto(mAcl))
        .build();
    return JournalEntry.newBuilder().setInodeFile(inodeFile).build();
=======
    return mDelegate.isCompleted();
>>>>>>> upstream-os/master
  }
}
