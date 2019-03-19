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

package alluxio.worker.security;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidCapabilityException;
import alluxio.proto.security.CapabilityProto;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authorization.Mode;
import alluxio.worker.block.BlockWorker;

/**
 * Netty data server related utils.
 */
public final class CapabilityUtils {
  private static final boolean CAPABILITY_ENABLED =
      ServerConfiguration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_ENABLED)
          && ServerConfiguration.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE)
              .equals(alluxio.security.authentication.AuthType.KERBEROS.getAuthName());

  /**
   * Checks whether the user has access to the given block.
   *
   * @param blockWorker the block worker
   * @param userInfo the authenticated user info
   * @param blockId the block ID
   * @param capability the capability to update if not null
   * @param accessMode the requested access mode
   * @throws InvalidCapabilityException if an invalid capability is found (e.g. expired). The client
   *         can retry the operation if an new Capability can be fetched.
   * @throws AccessControlException if the user does not have access to the block
   */
  public static void checkAccessMode(BlockWorker blockWorker, AuthenticatedUserInfo userInfo,
      long blockId, CapabilityProto.Capability capability, Mode.Bits accessMode)
      throws InvalidCapabilityException, AccessControlException {
    if (!CAPABILITY_ENABLED) {
      return;
    }
    long fileId = alluxio.util.IdUtils.fileIdFromBlockId(blockId);
    String user = userInfo.getAuthorizedUserName();
    blockWorker.getCapabilityCache().addCapability(capability);
    blockWorker.getCapabilityCache().checkAccess(user, fileId, accessMode);
  }

  /**
   * @return true if the capability feature is enabled
   */
  public static boolean isCapabilityEnabled() {
    return CAPABILITY_ENABLED;
  }

  private CapabilityUtils() {}  // prevent instantiation
}
