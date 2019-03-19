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

package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.security.authentication.AuthenticationServer;
import alluxio.security.authentication.DelegationTokenManager;
import alluxio.security.authentication.EnterpriseAuthenticationServer;
import alluxio.security.authentication.SaslServerHandler;
import alluxio.security.authentication.SaslServerHandlerDelegationToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;

/**
 * Enterprise {@link AuthenticationServer} implementation for Master processes.
 */
public class MasterAuthenticationServer extends EnterpriseAuthenticationServer {
  private static final Logger LOG = LoggerFactory.getLogger(MasterAuthenticationServer.class);

  private final DelegationTokenManager mDelegationTokenManager;

  /**
   * Creates an authentication server for worker process that supports Enterprise authentication
   * schemes.
   *
   * @param hostName host name of the server
   * @param tokenManager the delegation token manager
   * @param conf Alluxio configuration
   */
  public MasterAuthenticationServer(String hostName, DelegationTokenManager tokenManager,
      AlluxioConfiguration conf) {
    super(hostName, conf);
    mDelegationTokenManager = tokenManager;
  }

  @Override
  public SaslServerHandler createSaslHandler(ChannelAuthenticationScheme authScheme)
      throws SaslException, UnauthenticatedException {
    switch (authScheme) {
      case DELEGATION_TOKEN:
        return new SaslServerHandlerDelegationToken(mHostName, mDelegationTokenManager);
      default:
        return super.createSaslHandler(authScheme);
    }
  }
}
