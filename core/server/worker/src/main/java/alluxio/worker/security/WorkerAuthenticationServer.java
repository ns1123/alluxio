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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.security.authentication.AuthenticatedUserInfo;
import alluxio.security.authentication.AuthenticationServer;
import alluxio.security.authentication.EnterpriseAuthenticationServer;
import alluxio.security.authentication.SaslServerHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.util.UUID;

/**
 * Enterprise {@link AuthenticationServer} implementation for Worker processes.
 */
public class WorkerAuthenticationServer extends EnterpriseAuthenticationServer {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerAuthenticationServer.class);

  private final CapabilityCache mCapabilityCache;

  /**
   * Creates an authentication server for worker process that supports Enterprise authentication
   * schemes.
   *
   * @param hostName host name of the server
   * @param capabilityCache the capability cache
   * @param conf Alluxio configuration
   */
  public WorkerAuthenticationServer(String hostName, CapabilityCache capabilityCache,
      AlluxioConfiguration conf) {
    super(hostName, conf);
    mCapabilityCache = capabilityCache;
  }

  @Override
  public void registerChannel(UUID channelId, AuthenticatedUserInfo userInfo,
      SaslServer saslServer) {
    super.registerChannel(channelId, userInfo, saslServer);
    mCapabilityCache.incrementUserConnectionCount(userInfo.getAuthorizedUserName());
  }

  @Override
  public void unregisterChannel(UUID channelId) {
    try {
      mCapabilityCache
          .decrementUserConnectionCount(getUserInfoForChannel(channelId).getAuthorizedUserName());
    } catch (UnauthenticatedException e) {
      // Parent implementation will catch it.
    } finally {
      super.unregisterChannel(channelId);
    }
  }

  @Override
  public SaslServerHandler createSaslHandler(ChannelAuthenticationScheme authScheme)
      throws SaslException, UnauthenticatedException {
    switch (authScheme) {
      case CAPABILITY_TOKEN:
        if (!ServerConfiguration
            .getBoolean(PropertyKey.SECURITY_AUTHORIZATION_CAPABILITY_ENABLED)) {
          throw new UnauthenticatedException("Capability feature is disabled on server");
        }
        return new SaslServerHandlerCapabilityToken(mHostName, mCapabilityCache);
      default:
        return super.createSaslHandler(authScheme);
    }
  }
}
