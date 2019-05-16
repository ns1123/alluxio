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

package alluxio.security.authorization.hadoop;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.master.file.AccessControlEnforcer;
import alluxio.master.file.InodeAttributesProvider;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;
import alluxio.proto.journal.Journal;
import alluxio.proto.meta.InodeMeta;
import alluxio.security.authorization.AuthorizationPluginConstants;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.AclFeature;
import org.apache.hadoop.hdfs.server.namenode.AclStorage;
import org.apache.hadoop.hdfs.server.namenode.AlluxioHdfsINode;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.XAttrFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.AlluxioUserGroupInformation;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * An {@link InodeAttributesProvider} that allows Alluxio to retrieve inode attributes from
 * HDFS {@link org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider}.
 */
public class HdfsInodeAttributesProvider implements InodeAttributesProvider {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsInodeAttributesProvider.class);
  private final INodeAttributeProvider mHdfsProvider;
  private static boolean sIsAuthenticated;

  /**
   * Default constructor for Alluxio master to create {@link HdfsInodeAttributesProvider} instance.
   *
   * @param conf configuration for the plugin
   */
  public HdfsInodeAttributesProvider(UnderFileSystemConfiguration conf) {
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    if (conf != null) {
      conf.toMap().forEach((x, y) -> {
        if (y != null) {
          hadoopConf.set(x, Objects.toString(y, ""), "alluxio");
        }
      });
    }
    boolean isKerberized = SecurityUtil.getAuthenticationMethod(hadoopConf)
        == UserGroupInformation.AuthenticationMethod.KERBEROS;
    if (isKerberized) {
      try {
        login(conf);
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format("Failed while login with the configuration version %s: %s",
            AuthorizationPluginConstants.AUTH_VERSION, e.getMessage()), e);
      }
    }
    PrivilegedExceptionAction<INodeAttributeProvider> action = () ->  {
      INodeAttributeProvider provider;
      Class<? extends INodeAttributeProvider> klass = hadoopConf.getClass(
          DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
          null, INodeAttributeProvider.class);
      if (klass != null) {
        // attempts to load the provider if a class name is given in the hadoop configuration
        LOG.info("Loading INodeAttributeProvider from Hadoop configuration: {}, version {}",
            klass.getName(), AuthorizationPluginConstants.AUTH_VERSION);
        provider = ReflectionUtils.newInstance(klass, hadoopConf);
      } else {
        // falls back to using a ServiceLoader
        LOG.info("Loading INodeAttributeProvider using ServiceLoader: version {}",
            AuthorizationPluginConstants.AUTH_VERSION);
        ServiceLoader<INodeAttributeProvider> providers =
            ServiceLoader.load(INodeAttributeProvider.class);
        if (!providers.iterator().hasNext()) {
          throw new IllegalArgumentException(String.format(
              "Unable to get external HDFS INodeAttributeProvider version %s using ServiceLoader.",
              AuthorizationPluginConstants.AUTH_VERSION));
        }
        provider = providers.iterator().next();
        String className = provider.getClass().getName();
        LOG.info("Found INodeAttributeProvider using ServiceLoader: {}, version {}", className,
            AuthorizationPluginConstants.AUTH_VERSION);
        hadoopConf.set(DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY, className);
        ReflectionUtils.setConf(provider, hadoopConf);
      }
      return provider;
    };
    try {
      if (isKerberized) {
        // initializes the plugin using Kerberos credentials
        mHdfsProvider = UserGroupInformation.getLoginUser().doAs(action);
      } else {
        mHdfsProvider = action.run();
      }
    } catch (Exception e) {
      throw new IllegalStateException(String.format("Failed to create INodeAttributeProvider version %s: %s",
          AuthorizationPluginConstants.AUTH_VERSION, e.getMessage()), e);
    }
  }

  /**
   * Constructs {@link HdfsInodeAttributesProvider} for testing purpose.
   *
   * @param hdfsProvider an HDFS INodeAttributeProvider
   */
  HdfsInodeAttributesProvider(INodeAttributeProvider hdfsProvider) {
    mHdfsProvider = hdfsProvider;
  }

  INodeAttributeProvider getHdfsProvider() {
    return mHdfsProvider;
  }

  @Override
  public void start() {
    LOG.info("Starting HDFS INodeAttributesProvider: {}, version {}",
        mHdfsProvider.getClass().getName(), AuthorizationPluginConstants.AUTH_VERSION);
    mHdfsProvider.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping HDFS INodeAttributesProvider: {}, version {}",
        mHdfsProvider.getClass().getName(), AuthorizationPluginConstants.AUTH_VERSION);
    mHdfsProvider.stop();
  }

  @Override
  public InodeAttributes getAttributes(String[] pathElements, InodeAttributes inode) {
    return new HdfsAlluxioInodeAttributes(pathElements, inode);
  }

  @Override
  public AccessControlEnforcer getExternalAccessControlEnforcer(
      AccessControlEnforcer defaultEnforcer) {
    return new HdfsAccessControlEnforcer(defaultEnforcer);
  }

  /**
   * Logs in using Kerberos credentials from the given UFS configuration.
   */
  private void login(UnderFileSystemConfiguration ufsConf) throws IOException {
    String principal =
        ufsConf.get(PropertyKey.SECURITY_UNDERFS_HDFS_KERBEROS_CLIENT_PRINCIPAL);
    String keytab;
    if (principal.isEmpty()) {
      principal = ufsConf.get(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL);
      keytab = ufsConf.get(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE);
    } else {
      keytab = ufsConf.get(PropertyKey.SECURITY_UNDERFS_HDFS_KERBEROS_CLIENT_KEYTAB_FILE);
    }
    if (principal.isEmpty() || keytab.isEmpty()) {
      return;
    }
    synchronized (HdfsInodeAttributesProvider.class) {
      if (!sIsAuthenticated) {
        LOG.info("Login with principal: {} keytab: {}", principal, keytab);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
        sIsAuthenticated = true;
      } else {
        LOG.debug("Existing login with principal: {} keytab: {} existing ugi: {}",
            principal, keytab, UserGroupInformation.getLoginUser());
      }
    }
  }

  private class HdfsAccessControlEnforcer implements AccessControlEnforcer {
    private final INodeAttributeProvider.AccessControlEnforcer mHdfsAccessControlEnforcer;

    public HdfsAccessControlEnforcer(AccessControlEnforcer defaultEnforcer) {
      // creates an HDFS AccessControlEnforcer using a HDFS wrapper around the default enforcer
      mHdfsAccessControlEnforcer = mHdfsProvider.getExternalAccessControlEnforcer(
          new AlluxioHdfsAccessControlEnforcer(defaultEnforcer));
    }

    @Override
    public void checkPermission(String user, List<String> groups, Mode.Bits bits, String path,
        List<InodeView> inodeList, List<InodeAttributes> attributes, boolean checkIsOwner)
        throws alluxio.exception.AccessControlException {
      String fsOwner;
      try {
        fsOwner = ServerUserState.global().getUser().getName();
      } catch (UnauthenticatedException e) {
        throw new IllegalStateException("Failed to obtain login user.", e);
      }
      String superGroup =
          ServerConfiguration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_SUPERGROUP);
      UserGroupInformation callerUgi = new AlluxioUserGroupInformation(user, groups,
          ServerConfiguration.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE));
      byte[][] pathByNameArr;
      try {
        pathByNameArr = Arrays.stream(PathUtils.getPathComponents(path))
            .map(x -> x.getBytes()).toArray(byte[][]::new);
      } catch (InvalidPathException e) {
        throw new alluxio.exception.AccessControlException("Invalid inode path", e);
      }
      StringBuilder sb = new StringBuilder();
      // HDFS INode array is required to be of the same size as the path.
      // For INode not available in the path, the corresponding element should be set to null.
      INode[] hdfsInodes = new INode[pathByNameArr.length];
      INodeAttributes[] hdfsAttributes = new INodeAttributes[pathByNameArr.length];
      for (int i = 0; i < inodeList.size(); i++) {
        if (i > 0) {
          sb.append(AlluxioURI.SEPARATOR).append(new String(pathByNameArr[i]));
        }
        hdfsInodes[i] = AlluxioHdfsINode.create(inodeList.get(i), sb.toString());
        hdfsAttributes[i] = new AlluxioHdfsINodeAttributes(attributes.get(i));
      }
      // Magic number to tell HDFS to not check snapshot for any INode related computation
      int snapshotId = Snapshot.CURRENT_STATE_ID;
      // This is the same as HDFS FsPermissionChecker implementation but different than javadoc.
      // Plugins are expected to handle the case where the index is pointing to a null element.
      int ancestorIndex = hdfsInodes.length - 2;
      // Unlike HDFS, we don't check multiple access in a single checkPermission call.
      FsAction ancestorAccess = null;
      FsAction parentAccess = null;
      FsAction access = FsAction.getFsAction(bits == null ? null : bits.toString());
      FsAction subAccess = null;
      if (pathByNameArr.length > inodeList.size()) {
        // sets ancestor access action if the target node is not given
        ancestorAccess = access;
        access = null;
      }
      boolean ignoreEmptyDir = false;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Check HDFS plugin permission user={} groups={} path={} mode={}", user,
            Arrays.toString(groups.toArray()), path, bits);
      }
      try {
        mHdfsAccessControlEnforcer.checkPermission(fsOwner, superGroup,
            callerUgi, hdfsAttributes, hdfsInodes, pathByNameArr, snapshotId, path,
            ancestorIndex, checkIsOwner, ancestorAccess,
            parentAccess, access, subAccess, ignoreEmptyDir);
      } catch (AccessControlException e) {
        throw new alluxio.exception.AccessControlException(e.getMessage(), e);
      }
    }
  }

  class AlluxioHdfsAccessControlEnforcer
      implements INodeAttributeProvider.AccessControlEnforcer {
    private final AccessControlEnforcer mAccessPermissionEnforcer;

    public AlluxioHdfsAccessControlEnforcer(AccessControlEnforcer ace) {
      mAccessPermissionEnforcer = ace;
    }

    @Override
    public void checkPermission(String fsOwner, String superGroup, UserGroupInformation callerUgi,
        INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr, int snapshotId,
        String path, int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
        FsAction parentAccess, FsAction access, FsAction subAccess, boolean ignoreEmptyDir)
        throws AccessControlException {
      String user = callerUgi.getUserName();
      if (LOG.isDebugEnabled()) {
        LOG.debug(new StringBuilder().append("AlluxioHdfsAccessControlEnforcer.checkPermission(")
            .append(" fsOwner=").append(fsOwner)
            .append(" superGroup=").append(superGroup)
            .append(" inodesCount=").append(inodes != null ? inodes.length : 0)
            .append(" snapshotId=").append(snapshotId)
            .append(" user=").append(user)
            .append(" path=").append(path)
            .append(" ancestorIndex=").append(ancestorIndex)
            .append(" doCheckOwner=").append(doCheckOwner)
            .append(" ancestorAccess=").append(ancestorAccess)
            .append(" parentAccess=").append(parentAccess)
            .append(" access=").append(access)
            .append(" subAccess=").append(subAccess)
            .append(" ignoreEmptyDir=").append(ignoreEmptyDir)
            .append(")").toString());
      }
      if (isPermissionChecked(parentAccess)
          || isPermissionChecked(subAccess)) {
        // Plugins are not supposed to check a different inode with default enforcer.
        throw new AccessControlException("Checking parent node or sub node permission is not supported.");
      }
      FsAction targetAccess = isPermissionChecked(ancestorAccess) ? ancestorAccess : access;
      Mode.Bits bits = targetAccess == null ? null : Arrays.stream(Mode.Bits.values())
          .filter(x -> x.toString().equals(targetAccess.SYMBOL)).findFirst().get();
      List<String> groups = Arrays.asList(callerUgi.getGroupNames());
      // only adds non-null element to inode list
      List<InodeView> inodeList = Arrays.stream(inodes).filter(x -> x != null)
          .map(x -> getAlluxioInode(x)).collect(Collectors.toList());
      List<InodeAttributes> attributes = Arrays.stream(inodeAttrs).filter(x -> x != null)
          .map(x -> getAlluxioInodeAttributes(x)).collect(Collectors.toList());
      try {
        mAccessPermissionEnforcer.checkPermission(user, groups, bits, path, inodeList, attributes,
            doCheckOwner);
      } catch (alluxio.exception.AccessControlException e) {
        throw new AccessControlException(e);
      }
      LOG.debug("Passed default permission check {}, action={}", path, access);
    }

    private boolean isPermissionChecked(FsAction access) {
      return access != null && access != FsAction.NONE;
    }

    private InodeView getAlluxioInode(INode inode) {
      if (inode instanceof AlluxioHdfsINode) {
        // unwraps Alluxio Inode
        return ((AlluxioHdfsINode) inode).toAlluxioInode();
      }
      LOG.warn("Checking permission on non-Alluxio INodes: {}", inode.toDetailString());
      return new HdfsAlluxioInode(inode);
    }

    private InodeAttributes getAlluxioInodeAttributes(INodeAttributes attributes) {
      if (attributes instanceof AlluxioHdfsINodeAttributes) {
        // unwraps Alluxio InodeAttribute
        return ((AlluxioHdfsINodeAttributes) attributes).toAlluxioAttributes();
      }
      return new HdfsAlluxioInodeAttributes(attributes);
    }
  }

  /**
   * A wrapper class to provide HDFS {@link INode} information in Alluxio {@link Inode} interface.
   * It is used when HDFS authorization plugin is calling Alluxio permission checker to fallback to
   * default permission checking logic.
   */
  private class HdfsAlluxioInode extends MutableInode<HdfsAlluxioInode> {
    public HdfsAlluxioInode(INode hdfsINode) {
      super(Preconditions.checkNotNull(hdfsINode, "hdfsINode").getId(), hdfsINode.isDirectory());
      setName(hdfsINode.getLocalName());
      setOwner(hdfsINode.getUserName());
      setGroup(hdfsINode.getGroupName());
      setMode(hdfsINode.getFsPermissionShort());
      setLastModificationTimeMs(hdfsINode.getModificationTime(), true);
    }

    @Override
    public DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException {
      // TODO(feng): implement ACL support for plugin interface
      return new DefaultAccessControlList();
    }

    @Override
    public HdfsAlluxioInode setDefaultACL(DefaultAccessControlList acl)
        throws UnsupportedOperationException {
      // TODO(feng): implement ACL support for plugin interface
      return this;
    }

    @Override
    public FileInfo generateClientFileInfo(String path) {
      throw new UnsupportedOperationException(
          "HdfsAlluxioInode should not be used for client RPC calls.");
    }

    @Override
    public InodeMeta.Inode toProto() {
      throw new UnsupportedOperationException("HdfsAlluxioInode can't be acquired as proto");
    }

    @Override
    protected HdfsAlluxioInode getThis() {
      return this;
    }

    @Override
    public Journal.JournalEntry toJournalEntry(String path) {
      throw new UnsupportedOperationException("HdfsAlluxioInode should not be journaled.");
    }

    @Override
    public Journal.JournalEntry toJournalEntry() {
      throw new UnsupportedOperationException("HdfsAlluxioInode should not be journaled.");
    }
  }

  private class HdfsAlluxioInodeAttributes implements InodeAttributes {
    private final INodeAttributes mHdfsAttributes;

    public HdfsAlluxioInodeAttributes(String[] path, InodeAttributes attributes) {
      mHdfsAttributes = mHdfsProvider.getAttributes(path,
          new AlluxioHdfsINodeAttributes(attributes));
    }

    public HdfsAlluxioInodeAttributes(INodeAttributes hdfsAttributes) {
      mHdfsAttributes = hdfsAttributes;
    }

    @Override
    public boolean isDirectory() {
      return mHdfsAttributes.isDirectory();
    }

    @Override
    public String getName() {
      return new String(mHdfsAttributes.getLocalNameBytes(), Charset.defaultCharset());
    }

    @Override
    public String getOwner() {
      return mHdfsAttributes.getUserName();
    }

    @Override
    public String getGroup() {
      return mHdfsAttributes.getGroupName();
    }

    @Override
    public short getMode() {
      return mHdfsAttributes.getFsPermissionShort();
    }

    @Override
    public long getLastModificationTimeMs() {
      return mHdfsAttributes.getModificationTime();
    }

    @Override
    public String toString() {
      return String.format("Inode:%s [u]%s [g]%s [m]%s [acl]%s",
          getName(),
          getOwner(),
          getGroup(),
          getMode(),
          Arrays.toString(AclStorage.readINodeAcl(mHdfsAttributes).toArray()));
    }
  }

  private class AlluxioHdfsINodeAttributes implements INodeAttributes {
    private final InodeAttributes mAttributes;

    public AlluxioHdfsINodeAttributes(InodeAttributes attributes) {
      mAttributes = attributes;
    }

    @Override
    public boolean isDirectory() {
      return mAttributes.isDirectory();
    }

    @Override
    public byte[] getLocalNameBytes() {
      return mAttributes.getName().getBytes();
    }

    @Override
    public String getUserName() {
      return mAttributes.getOwner();
    }

    @Override
    public String getGroupName() {
      return mAttributes.getGroup();
    }

    @Override
    public FsPermission getFsPermission() {
      return FsPermission.createImmutable(mAttributes.getMode());
    }

    @Override
    public short getFsPermissionShort() {
      return mAttributes.getMode();
    }

    @Override
    public long getPermissionLong() {
      return mAttributes.getMode();
    }

    @Override
    public AclFeature getAclFeature() {
      // TODO(feng): Sync and plugin ACL
      return null;
    }

    @Override
    public XAttrFeature getXAttrFeature() {
      return null;
    }

    @Override
    public long getModificationTime() {
      return mAttributes.getLastModificationTimeMs();
    }

    @Override
    public long getAccessTime() {
      return mAttributes.getLastModificationTimeMs();
    }

    @Override
    public String toString() {
      return mAttributes.toString();
    }

    public InodeAttributes toAlluxioAttributes() {
      return mAttributes;
    }
  }
}
