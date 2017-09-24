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

package alluxio.worker.netty;

import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidCapabilityException;
import alluxio.proto.security.CapabilityProto;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsManager;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockWorker;

import io.netty.channel.ChannelHandlerContext;

/**
 * Netty data server related utils.
 */
public final class Utils {
  private static final boolean CAPABILITY_ENABLED = alluxio.Configuration
      .getBoolean(alluxio.PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_ENABLED)
      && alluxio.Configuration.get(alluxio.PropertyKey.SECURITY_AUTHENTICATION_TYPE)
      .equals(alluxio.security.authentication.AuthType.KERBEROS.getAuthName());

  /** Magic number to write UFS block to UFS. */
  private static final String MAGIC_NUMBER = "1D91AC0E";

  /**
   * Checks whether the user has access to the given block.
   *
   * @param blockWorker the block worker
   * @param ctx the netty handler context
   * @param blockId the block ID
   * @param capability the capability to update if not null
   * @param accessMode the requested access mode
   * @throws InvalidCapabilityException if an invalid capability is found (e.g. expired). The client
   *         can retry the operation if an new Capability can be fetched.
   * @throws AccessControlException if the user does not have access to the block
   */
  public static void checkAccessMode(BlockWorker blockWorker, ChannelHandlerContext ctx,
      long blockId, CapabilityProto.Capability capability, Mode.Bits accessMode)
      throws InvalidCapabilityException, AccessControlException {
    if (!CAPABILITY_ENABLED) {
      return;
    }
    long fileId = alluxio.util.IdUtils.fileIdFromBlockId(blockId);
    String user = ctx.channel().attr(alluxio.netty.NettyAttributes.CHANNEL_KERBEROS_USER_KEY).get();
    blockWorker.getCapabilityCache().addCapability(capability);
    blockWorker.getCapabilityCache().checkAccess(user, fileId, accessMode);
  }

  /**
   * @return true if the capability feature is enabled
   */
  public static boolean isCapabilityEnabled() {
    return CAPABILITY_ENABLED;
  }

  /**
   * For a given block ID, derives the corresponding UFS file of this block if it falls back to
   * be stored in UFS.
   *
   * @param ufsInfo the UFS information for the mount point backing this file
   * @param blockId block ID
   * @return the UFS path of a block
   */
  public static String getUfsBlockPath(UfsManager.UfsInfo ufsInfo, long blockId) {
    return PathUtils.concatPath(ufsInfo.getUfsMountPointUri(),
        String.format(".alluxio_blocks_%s/%s/", MAGIC_NUMBER, blockId));
  }

  private Utils() {}  // prevent instantiation
}
