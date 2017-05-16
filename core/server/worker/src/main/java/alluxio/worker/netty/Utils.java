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
import alluxio.worker.block.BlockWorker;

import io.netty.channel.ChannelHandlerContext;

/**
 * Netty data server related utils.
 */
public final class Utils {

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
    long fileId = alluxio.util.IdUtils.fileIdFromBlockId(blockId);
    String user = ctx.channel().attr(alluxio.netty.NettyAttributes.CHANNEL_KERBEROS_USER_KEY).get();
    blockWorker.getCapabilityCache().addCapability(capability);
    blockWorker.getCapabilityCache().checkAccess(user, fileId, accessMode);
  }

  private Utils() {}  // prevent instantiation
}
