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

import alluxio.exception.status.UnauthenticatedException;
import alluxio.network.thrift.ThriftUtils;
import alluxio.security.LoginUser;
import alluxio.security.util.KerberosUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;

/**
 * SASL transport provider when authentication type is {@link AuthType#KERBEROS).
 */
@ThreadSafe
public final class KerberosSaslTransportProvider implements TransportProvider {
  public static final Logger LOG = LoggerFactory.getLogger(KerberosSaslTransportProvider.class);

  /**
   * Constructor for transport provider when authentication type is {@link AuthType#KERBEROS).
   */
  public KerberosSaslTransportProvider() {}

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    return getClientTransport(null, serverAddress);
  }

  @Override
  public TTransport getClientTransport(Subject subject, InetSocketAddress serverAddress)
      throws UnauthenticatedException {
    if (subject == null) {
      subject = LoginUser.getClientLoginSubject();
    }
    String serviceName = KerberosUtils.getKerberosServiceName();
    return getClientTransportInternal(
        subject, serviceName, serverAddress.getHostName(), serverAddress);
  }

  /**
   * Gets a client thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serverName Thrift SASL server name
   * @param serverAddress thrift server address
   * @return Thrift transport
   */
  public TTransport getClientTransportInternal(
      Subject subject, final String protocol, final String serverName,
      final InetSocketAddress serverAddress) throws UnauthenticatedException {
    String unifiedInstanceName = KerberosUtils.maybeGetKerberosUnifiedInstanceName();
    final String instanceName = unifiedInstanceName != null ? unifiedInstanceName : serverName;

    Token<DelegationTokenIdentifier> token = KerberosUtils.getDelegationToken(subject,
        HostAndPort.fromParts(serverAddress.getAddress().getHostAddress(), serverAddress.getPort())
            .toString());
    LOG.debug("Delegation token found for subject {} and server {}: {}.", subject, serverAddress,
        token);
    // Determine the impersonation user
    String impersonationUser = TransportProviderUtils.getImpersonationUser(subject);

    try {
      return Subject.doAs(subject, new
          PrivilegedExceptionAction<TSaslClientTransport>() {
        public TSaslClientTransport run() throws AuthenticationException {
          try {
            TTransport wrappedTransport =
                ThriftUtils.createThriftSocket(serverAddress);
            if (token != null) {
              LOG.debug("Use delegation token authentication.");
              return new TSaslClientTransport(
                  KerberosUtils.DIGEST_MECHANISM_NAME, null,
                  protocol, instanceName, KerberosUtils.SASL_PROPERTIES,
                  new KerberosUtils.SaslDigestClientCallbackHandler(token), wrappedTransport);
            }
            LOG.debug("Use Kerberos authentication.");
            return new TSaslClientTransport(
                KerberosUtils.GSSAPI_MECHANISM_NAME, impersonationUser,
                protocol, instanceName, KerberosUtils.SASL_PROPERTIES, null, wrappedTransport);
          } catch (SaslException e) {
            throw new AuthenticationException("Exception initializing SASL client", e);
          }
        }
      });
    } catch (PrivilegedActionException e) {
      Throwables.propagateIfPossible(e.getCause(), UnauthenticatedException.class);
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public TTransportFactory getServerTransportFactory(String serverName) throws SaslException {
    return getServerTransportFactory(() -> { }, serverName);
  }

  @Override
  public TTransportFactory getServerTransportFactory(Runnable runnable, String serverName)
      throws SaslException {
    return getServerTransportFactory(() -> { }, serverName, null);
  }

  @Override
  public TTransportFactory getServerTransportFactory(String serverName,
      DelegationTokenManager tokenManager)
      throws SaslException {
    return getServerTransportFactory(() -> { }, serverName, tokenManager);
  }

  private TTransportFactory getServerTransportFactory(Runnable runnable, String serverName,
      DelegationTokenManager tokenManager)
      throws SaslException {
    try {
      Subject subject = LoginUser.getServerLoginSubject();
      String serviceName = KerberosUtils.getKerberosServiceName();
      Preconditions.checkNotNull(serverName);
      return getServerTransportFactoryInternal(subject, serviceName, serverName, runnable,
          tokenManager);
    } catch (IOException | PrivilegedActionException e) {
      throw new SaslException("Failed to create KerberosSaslServer : ", e);
    }
  }

  /**
   * Gets a server thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serverName Thrift SASL server name
   * @param callback the callback runs after the transport is established
   * @return a server transport
   * @throws SaslException when SASL can't be initialized
   * @throws PrivilegedActionException when the Subject doAs failed
   */
  public TTransportFactory getServerTransportFactoryInternal(Subject subject, final String protocol,
      final String serverName, final Runnable callback)
      throws SaslException, PrivilegedActionException {
    return getServerTransportFactoryInternal(subject, protocol, serverName, callback, null);
  }

  /**
   * Gets a server thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serverName Thrift SASL server name
   * @param callback the callback runs after the transport is established
   * @param tokenManager the delegation token manager
   * @return a server transport
   * @throws SaslException when SASL can't be initialized
   * @throws PrivilegedActionException when the Subject doAs failed
   */
  private TTransportFactory getServerTransportFactoryInternal(Subject subject, final String protocol,
      final String serverName, final Runnable callback, final DelegationTokenManager tokenManager)
      throws SaslException, PrivilegedActionException {
    String unifiedInstanceName = KerberosUtils.maybeGetKerberosUnifiedInstanceName();
    final String instanceName = unifiedInstanceName != null ? unifiedInstanceName : serverName;
    return Subject.doAs(subject, new PrivilegedExceptionAction<TSaslServerTransport.Factory>() {
      public TSaslServerTransport.Factory run() {
        TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
        saslTransportFactory
            .addServerDefinition(KerberosUtils.GSSAPI_MECHANISM_NAME, protocol, instanceName,
                KerberosUtils.SASL_PROPERTIES,
                new KerberosUtils.ThriftGssSaslCallbackHandler(callback));
        if (tokenManager != null) {
          LOG.debug("Delegation token authentication enabled.");
          saslTransportFactory
              .addServerDefinition(KerberosUtils.DIGEST_MECHANISM_NAME, protocol, instanceName,
                  KerberosUtils.SASL_PROPERTIES,
                  new KerberosUtils.ThriftDelegationTokenServerCallbackHandler(callback,
                      tokenManager));
        }
        return saslTransportFactory;
      }
    });
  }
}
