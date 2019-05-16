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

package alluxio.security.authentication;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.security.authentication.kerberos.SaslServerHandlerKerberos;
import alluxio.security.user.UserState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;

/**
 * Enterprise implementation of {@link AuthenticationServer}.
 */
public class EnterpriseAuthenticationServer extends DefaultAuthenticationServer {
  private static final Logger LOG = LoggerFactory.getLogger(EnterpriseAuthenticationServer.class);

  // TODO(gpang): Can we move this class to core/server/common, so it can use ServerUserState?
  // The user state for the server identity
  private final UserState mServerUserState;

  /**
   * Creates an authentication server for worker process that supports Enterprise authentication
   * schemes.
   *
   * @param hostName host name of the server
   * @param conf Alluxio configuration
   * @param serverUserState the user state for the server
   */
  public EnterpriseAuthenticationServer(String hostName, AlluxioConfiguration conf,
      UserState serverUserState) {
    super(hostName, conf);
    mServerUserState = serverUserState;
  }

  @Override
  public SaslServerHandler createSaslHandler(ChannelAuthenticationScheme authScheme)
      throws SaslException, UnauthenticatedException {
    switch (authScheme) {
      case KERBEROS:
        return new SaslServerHandlerKerberos(mHostName, mServerUserState.getSubject(),
            mConfiguration);
      default:
        return super.createSaslHandler(authScheme);
    }
  }

  @Override
  protected void checkSupported(AuthType authType) {
    switch (authType) {
      case KERBEROS:
        break;
      default:
        super.checkSupported(authType);
    }
  }
}
