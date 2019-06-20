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

package alluxio.master.policy.action.data;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.conf.ServerConfiguration;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.MountTable;
import alluxio.master.policy.action.ActionExecutionContext;
import alluxio.master.policy.action.CommitActionExecution;
import alluxio.master.policy.meta.InodeState;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The action to persist a directory from Alluxio to one UFS or a group sub UFSes (at least 1)
 * in one union UFS.
 * If the directory already exists, the action succeeds.
 */
@ThreadSafe
public final class UfsDirStoreActionExecution extends CommitActionExecution {
  private static final Logger LOG = LoggerFactory.getLogger(UfsDirStoreActionExecution.class);

  private final Set<String> mSubUfses;

  /**
   * @param ctx the execution context
   * @param path the Alluxio path
   * @param inode the inode
   * @param subUfses the list of sub UFS modifiers when storing to a group of sub UFSes in one union
   *    UFS or empty when storing to one UFS
   */
  public UfsDirStoreActionExecution(ActionExecutionContext ctx, String path, InodeState inode,
      Set<String> subUfses) {
    super(ctx, path, inode);
    mSubUfses = subUfses;
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  @Override
  protected void doCommit() throws Exception {
    if (mInode.isMountPoint()) {
      // UFS:STORE on a mount point is a noop.
      return;
    }

    mContext.getFileSystemMaster().exec(mInode.getId(), LockPattern.WRITE_INODE, context -> {
      MountTable.Resolution resolution = context.getMountInfo();
      Set<String> persistedSubUfses = new HashSet<>(mSubUfses.size());
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();

        // Generate a map from the ufs path to its sub ufs name.
        Map<String, String> ufsPaths = new HashMap<>(mSubUfses.size());
        AlluxioURI ufsUri = resolution.getUri();
        if (mSubUfses.isEmpty()) {
          // Store to one UFS.
          ufsPaths.put(ufsUri.toString(), "");
        } else {
          // Store to a group of sub UFSes.
          if (ufsUri.getScheme() == null || !DataActionUtils.isUnionUfs(ufsUri)) {
            throw new IllegalStateException(String.format("%s is not union UFS", ufsUri.toString()));
          }
          for (String subUfs : mSubUfses) {
            String subUfsUri = DataActionUtils.createUnionSubUfsUri(ufsUri, subUfs);
            ufsPaths.put(subUfsUri, subUfs);
          }
        }

        for (Map.Entry<String, String> entry : ufsPaths.entrySet()) {
          String ufsPath = entry.getKey();
          String subUfsName = entry.getValue();

          // Create ancestor directories from top to the bottom. We cannot use recursive create
          // parents here because the permission for the ancestors can be different.

          // Each item in the ancestors stack is a pair of UFS ancestor directory to make and the
          // Alluxio inode corresponding to that directory.
          Stack<Pair<String, Inode>> ancestors = new Stack<>();
          List<Inode> inodes = context.getInodes();
          int curInodeIndex = inodes.size() - 1;
          AlluxioURI curUfsPath = new AlluxioURI(ufsPath);
          // Stop when the directory already exists in UFS.
          while (!ufs.isDirectory(curUfsPath.toString()) && curInodeIndex >= 0) {
            Inode curInode = inodes.get(curInodeIndex);
            ancestors.push(new Pair<>(curUfsPath.toString(), curInode));
            curUfsPath = curUfsPath.getParent();
            curInodeIndex--;
          }

          while (!ancestors.empty()) {
            Pair<String, Inode> ancestor = ancestors.pop();
            String dir = ancestor.getFirst();
            Inode inode = ancestor.getSecond();
            MkdirsOptions options = MkdirsOptions.defaults(ServerConfiguration.global())
                .setCreateParent(false)
                .setOwner(inode.getOwner())
                .setGroup(inode.getGroup())
                .setMode(new Mode(inode.getMode()));
            // UFS mkdirs might fail if the directory is already created. If so, skip the mkdirs
            // and assume the directory is already prepared, regardless of permission matching.
            try {
              if (ufs.mkdirs(dir, options)) {
                LOG.debug("UFS directory {} is created", dir);
                List<AclEntry> allAcls = Stream.concat(inode.getDefaultACL().getEntries().stream(),
                    inode.getACL().getEntries().stream()).collect(Collectors.toList());
                ufs.setAclEntries(dir, allAcls);
                persistedSubUfses.add(subUfsName);
              } else {
                if (ufs.isDirectory(dir)) {
                  LOG.debug("UFS directory {} already exists", dir);
                } else {
                  throw new IOException(String.format("UFS path %s is an existing file", dir));
                }
              }
            } catch (IOException e) {
              LOG.error("Failed to create UFS directory {} with correct permission", dir, e);
              throw e;
            }
          }
        }
      } finally {
        if (!persistedSubUfses.isEmpty()) {
          persistedSubUfses.remove("");
          context.propagatePersisted(persistedSubUfses);
        }
      }
    });
  }

  @Override
  public String getDescription() {
    return mSubUfses.stream().map(subUfs -> String.format("UFS[%s]:STORE",
        subUfs, mInode.getId())).collect(Collectors.joining(", "));
  }
}
