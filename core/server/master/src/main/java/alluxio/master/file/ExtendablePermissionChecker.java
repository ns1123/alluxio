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

package alluxio.master.file;

import alluxio.exception.AccessControlException;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MutableInode;
import alluxio.proto.journal.Journal;
import alluxio.proto.meta.InodeMeta;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A permission checker allows plugins to provide custom permission checking logic.
 */
public final class ExtendablePermissionChecker extends DefaultPermissionChecker
    implements AccessControlEnforcer {

  private InodeAttributesProvider mProvider = null;
  private AccessControlEnforcer mAccessControlEnforcer = this;

  /**
   * Constructs an {@link ExtendablePermissionChecker} instance for Alluxio file system.
   *
   * @param inodeTree inode tree of the file system master
   * @param attributesProvider an external provider used for overriding inode attributes or
   *                          permission checking
   */
  public ExtendablePermissionChecker(InodeTree inodeTree,
      InodeAttributesProvider attributesProvider) {
    super(inodeTree);
    mProvider = Preconditions.checkNotNull(attributesProvider, "attributesProvider");
    mAccessControlEnforcer = Preconditions.checkNotNull(
        mProvider.getExternalAccessControlEnforcer(this),
        "AccessControlEnforcer");
  }

  @Override
  public void checkPermission(String user, List<String> groups, Mode.Bits bits, String path,
      List<InodeView> inodeList, List<InodeAttributes> attributes,
      boolean checkIsOwner) throws AccessControlException {
    Preconditions.checkArgument(inodeList.size() == attributes.size(),
        "Overridden attributes count should be equal to the inode count");
    // combines inodes with overridden attributes
    List<InodeView> newInodeList = IntStream.range(0, inodeList.size())
        .mapToObj(i -> getOverriddenInode(inodeList.get(i), attributes.get(i)))
        .collect(Collectors.toList());
    // calls the default permission checking method because the external AccessControlEnforcer
    // is not available or could not determine the permission.
    super.checkInodeList(user, groups, bits, path, newInodeList, checkIsOwner);
  }

  // This function is called by DefaultPermissionChecker to check permission on a list of inode
  // representing a path. It will call the original checkInodeList if the external
  // AccessControlEnforcer cannot determine the permission.
  @Override
  protected void checkInodeList(String user, List<String> groups, Mode.Bits bits, String path,
      List<InodeView> inodeList, boolean checkIsOwner) throws AccessControlException {
    List<InodeAttributes> attributesList;
    List<String> pathComponents = new ArrayList<>();
    attributesList = new ArrayList<>();
    for (InodeView inode : inodeList) {
      pathComponents.add(inode.getName());
      // get the overridden attributes from the provider
      attributesList.add(mProvider.getAttributes(
          pathComponents.toArray(new String[0]), new DefaultInodeAttributes(inode)));
    }
    mAccessControlEnforcer.checkPermission(
        user, groups, bits, path, inodeList, attributesList, checkIsOwner);
  }

  @Override
  public Mode.Bits getPermission(LockedInodePath inodePath, Mode.Bits requestedMode) {
    if (isSuperUser()) {
      return Mode.Bits.ALL;
    }
    if (requestedMode == null) {
      return Mode.Bits.NONE;
    }
    Mode.Bits grantedMode = Mode.Bits.NONE;
    for (AclAction action : requestedMode.toAclActionSet()) {
      try {
        Mode.Bits mode = action.toModeBits();
        checkPermission(action.toModeBits(), inodePath);
        grantedMode = grantedMode.or(mode);
      } catch (AccessControlException e) {
        // this is expected if user is not granted target access by the permission checker
      }
    }
    return grantedMode;
  }

  private InodeView getOverriddenInode(InodeView inode, InodeAttributes attributes) {
    if (inode == getOriginalInode(attributes)) {
      return inode;
    }
    return new InodeWithOverridenAttributes(inode, attributes);
  }

  private InodeView getOriginalInode(InodeAttributes attributes) {
    if (attributes != null && attributes instanceof DefaultInodeAttributes) {
      return ((DefaultInodeAttributes) attributes).getInode();
    }
    return null;
  }

  /**
   * Default implementation of {@link InodeAttributes} with attribute values from an {@link Inode}.
   */
  public static class DefaultInodeAttributes implements InodeAttributes {
    private final InodeView mInode;

    /**
     * Default constructor.
     * @param inode an inode that provides all attribute values
     */
    public DefaultInodeAttributes(InodeView inode) {
      mInode = Preconditions.checkNotNull(inode, "inode");
    }

    @Override
    public boolean isDirectory() {
      return mInode.isDirectory();
    }

    @Override
    public String getName() {
      return mInode.getName();
    }

    @Override
    public String getOwner() {
      return mInode.getOwner();
    }

    @Override
    public String getGroup() {
      return mInode.getGroup();
    }

    @Override
    public short getMode() {
      return mInode.getMode();
    }

    @Override
    public long getLastModificationTimeMs() {
      return mInode.getLastModificationTimeMs();
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this || (obj != null && obj instanceof DefaultInodeAttributes
          && ((DefaultInodeAttributes) obj).mInode == this.mInode);
    }

    @Override
    public int hashCode() {
      return mInode.hashCode();
    }

    /**
     * @return the source {@link Inode}
     */
    public InodeView getInode() {
      return mInode;
    }
  }

  private class InodeWithOverridenAttributes extends MutableInode<InodeWithOverridenAttributes> {
    private final InodeView mInode;
    private final InodeAttributes mAttributes;

    public InodeWithOverridenAttributes(InodeView inode,
        InodeAttributes attributes) {
      super(inode.getId(), inode.isDirectory());
      mInode = inode;
      mAttributes = attributes;
      setName(mAttributes.getName());
      setMode(mAttributes.getMode());
      setOwner(mAttributes.getOwner());
      setGroup(mAttributes.getGroup());
      setLastModificationTimeMs(mAttributes.getLastModificationTimeMs());
    }

    @Override
    public alluxio.wire.FileInfo generateClientFileInfo(String path) {
      return mInode.generateClientFileInfo(path);
    }

    @Override
    public InodeMeta.Inode toProto() {
      throw new UnsupportedOperationException(
          "InodeWithOverridenAttributes can't be acquired as proto");
    }

    @Override
    protected InodeWithOverridenAttributes getThis() {
      return this;
    }

    @Override
    public Journal.JournalEntry toJournalEntry() {
      return mInode.toJournalEntry();
    }

    @Override
    public boolean isDirectory() {
      return mAttributes.isDirectory();
    }

    @Override
    public DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException {
      // TODO(feng): implement ACL support for plugin interface
      return new DefaultAccessControlList();
    }

    @Override
    public InodeWithOverridenAttributes setDefaultACL(DefaultAccessControlList acl)
        throws UnsupportedOperationException {
      // TODO(feng): implement ACL support for plugin interface
      return this;
    }
  }
}
