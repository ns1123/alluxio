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

package alluxio.security.authentication.kerberos;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.security.LoginUser;
import alluxio.security.authentication.SaslClientHandler;
import alluxio.security.authentication.AuthenticationUserUtils;
import alluxio.security.util.KerberosUtils;

import com.google.common.base.Throwables;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.net.InetSocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Creates {@link SaslClientHandler} instance for Kerberos.
 */
public class SaslClientHandlerKerberos implements SaslClientHandler {

  /** Underlying SaslClient. */
  private final SaslClient mSaslClient;

  private final Subject mSubject;

  /**
   * Creates {@link SaslClientHandler} instance for Kerberos.
   *
   * @param subject client subject
   * @param serverAddress target server address
   * @param conf Alluxio configuration
   * @throws UnauthenticatedException
   */
  public SaslClientHandlerKerberos(Subject subject, InetSocketAddress serverAddress,
      AlluxioConfiguration conf) throws UnauthenticatedException {
    // Get the login user if given subject is null.
    if (subject == null) {
      subject = LoginUser.getClientLoginSubject(conf);
    }
    mSubject = subject;

    String unifiedInstanceName = KerberosUtils.maybeGetKerberosUnifiedInstanceName(conf);
    final String instanceName = unifiedInstanceName != null ? unifiedInstanceName
        : serverAddress.getAddress().getHostName();

    // Determine the impersonation user
    String impersonationUser = AuthenticationUserUtils.getImpersonationUser(subject, conf);

    try {
      SaslClient saslClient = Subject.doAs(subject, new PrivilegedExceptionAction<SaslClient>() {
        public SaslClient run() throws UnauthenticatedException {
          try {
            return Sasl.createSaslClient(new String[] {KerberosUtils.GSSAPI_MECHANISM_NAME},
                impersonationUser, KerberosUtils.getKerberosServiceName(conf), instanceName,
                KerberosUtils.SASL_PROPERTIES, null);
          } catch (SaslException e) {
            throw new UnauthenticatedException(e.getMessage(), e);
          }
        }
      });
      mSaslClient = new PrivilegedSaslClient(saslClient, mSubject);
    } catch (PrivilegedActionException pe) {
      Throwables.propagateIfPossible(pe.getCause(), UnauthenticatedException.class);
      throw new RuntimeException(pe.getCause());
    }
  }

  @Override
  public ChannelAuthenticationScheme getClientScheme() {
    return ChannelAuthenticationScheme.KERBEROS;
  }

  @Override
  public SaslClient getSaslClient() {
    return mSaslClient;
  }
}
