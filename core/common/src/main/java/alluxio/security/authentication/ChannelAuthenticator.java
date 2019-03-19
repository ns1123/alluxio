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
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.grpc.ChannelAuthenticationScheme;
import alluxio.exception.status.UnknownException;
import alluxio.grpc.SaslAuthenticationServiceGrpc;
import alluxio.grpc.SaslMessage;
import alluxio.grpc.GrpcChannelBuilder;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;

/**
 * Used to authenticate with the target host. Used internally by {@link GrpcChannelBuilder}.
 */
public class ChannelAuthenticator {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelAuthenticator.class);

  /** Alluxio client configuration. */
  private AlluxioConfiguration mConfiguration;

  /** Whether to use mParentSubject as authentication user. */
  protected boolean mUseSubject;
  /** Subject for authentication. */
  protected Subject mParentSubject;

  /* Used in place of a subject. */
  protected String mUserName;
  protected String mPassword;
  protected String mImpersonationUser;

  /** Authentication type to use with the target host. */
  protected AuthType mAuthType;

  /** gRPC Authentication timeout in milliseconds. */
  protected final long mGrpcAuthTimeoutMs;

  /** Internal ID used to identify the channel that is being authenticated. */
  protected UUID mChannelId;

  /**
   * Creates {@link ChannelAuthenticator} instance.
   *
   * @param subject javax subject to use for authentication
   * @param conf Alluxio configuration
   */
  public ChannelAuthenticator(Subject subject, AlluxioConfiguration conf) {
    mUseSubject = true;
    mParentSubject = subject;
    mConfiguration = conf;
    mChannelId = UUID.randomUUID();
    mAuthType = conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
    mGrpcAuthTimeoutMs = conf.getMs(PropertyKey.MASTER_GRPC_CHANNEL_AUTH_TIMEOUT);
  }

  /**
   * Creates {@link ChannelAuthenticator} instance.
   *
   * @param userName user name
   * @param password user password
   * @param impersonationUser impersonation user
   * @param authType authentication type
   * @param grpcAuthTimeoutMs authentication timeout in milliseconds
   */
  public ChannelAuthenticator(String userName, String password, String impersonationUser,
      AuthType authType, long grpcAuthTimeoutMs) {
    mUseSubject = false;
    mChannelId = UUID.randomUUID();
    mUserName = userName;
    mPassword = password;
    mImpersonationUser = impersonationUser;
    mAuthType = authType;
    mGrpcAuthTimeoutMs = grpcAuthTimeoutMs;
  }

  /**
   * Authenticates given {@link NettyChannelBuilder} instance. It attaches required interceptors to
   * the channel based on authentication type.
   *
   * @param serverAddress the remote address to which the given channel has been opened
   * @param managedChannel the managed channel for whch authentication is taking place
   * @return channel that is augmented for authentication
   * @throws UnauthenticatedException
   */
  public Channel authenticate(SocketAddress serverAddress, ManagedChannel managedChannel)
      throws AlluxioStatusException {
    LOG.debug("Channel authentication initiated. ChannelId:{}, AuthType:{}, Target:{}", mChannelId,
            mAuthType, managedChannel.authority());

    if (mAuthType == AuthType.NOSASL) {
      return managedChannel;
    }

    return new AuthenticatedManagedChannel(serverAddress, managedChannel);
  }

  private class AuthenticatedManagedChannel extends Channel implements AuthenticatedChannel {
    private final SocketAddress mServerAddress;
    private final ManagedChannel mManagedChannel;
    private Channel mChannel;
    private boolean mAuthenticated;

    AuthenticatedManagedChannel(SocketAddress serverAddress, ManagedChannel managedChannel)
        throws AlluxioStatusException {
      mServerAddress = serverAddress;
      mManagedChannel = managedChannel;
      authenticate();
      mAuthenticated = true;
    }

    public void authenticate() throws AlluxioStatusException {
      try {
        // Determine channel authentication scheme to use.
        ChannelAuthenticationScheme authScheme =
            getChannelAuthScheme(mParentSubject, mServerAddress);
        // Create SaslHandler for talking with target host's authentication service.
        SaslClientHandler saslClientHandler =
            createSaslClientHandler(mServerAddress, authScheme, mParentSubject);
        // Create authentication scheme specific handshake handler.
        SaslHandshakeClientHandler handshakeClient =
            new DefaultSaslHandshakeClientHandler(saslClientHandler);
        // Create driver for driving sasl traffic from client side.
        SaslStreamClientDriver clientDriver =
            new SaslStreamClientDriver(handshakeClient, mGrpcAuthTimeoutMs);
        // Start authentication call with the service and update the client driver.
        StreamObserver<SaslMessage> requestObserver =
            SaslAuthenticationServiceGrpc.newStub(mManagedChannel).authenticate(clientDriver);
        clientDriver.setServerObserver(requestObserver);
        // Start authentication traffic with the target.
        clientDriver.start(mChannelId.toString());
        // Authentication succeeded!
        // Intercept authenticated channel with channel-Id injector.
        mChannel = ClientInterceptors.intercept(mManagedChannel, new ChannelIdInjector(mChannelId));
      } catch (Exception exc) {
        String message = String.format(
            "Channel authentication failed. ChannelId: %s, AuthType: %s, Target: %s, Error: %s",
            mChannelId, mAuthType, mManagedChannel.authority(), exc.toString());
        if (exc instanceof AlluxioStatusException) {
          throw AlluxioStatusException.from(((AlluxioStatusException) exc).getStatus(), message,
              exc);
        } else {
          throw new UnknownException(message, exc);
        }
      }
    }

    /**
     * Determines transport level authentication scheme for given subject.
     *
     * @param subject the subject
     * @param serverAddress the target server address
     * @return the channel authentication scheme to use
     * @throws UnauthenticatedException if configured authentication type is not supported
     */
    private ChannelAuthenticationScheme getChannelAuthScheme(Subject subject,
        SocketAddress serverAddress) throws UnauthenticatedException {
      switch (mAuthType) {
        case NOSASL:
          return ChannelAuthenticationScheme.NOSASL;
        case SIMPLE:
          return ChannelAuthenticationScheme.SIMPLE;
        case CUSTOM:
          return ChannelAuthenticationScheme.CUSTOM;
        // ALLUXIO CS ADD
        case KERBEROS:
          Preconditions.checkArgument(serverAddress instanceof InetSocketAddress, String.format(
                  "Need an Inet host for auth type: %s. Found: %s", mAuthType, serverAddress.getClass()));
          InetSocketAddress serverInetAddress = (InetSocketAddress) serverAddress;

          // Check if the subject contains a capability token.
          if (subject != null) {
            alluxio.security.authentication.Token<?> capabilityToken =
                    alluxio.security.authentication.token.TokenUtils
                            .getCapabilityTokenFromSubject(subject);
            if (capabilityToken != null) {
              return ChannelAuthenticationScheme.CAPABILITY_TOKEN;
            }
          }
          // Check if the subject contains a delegation token.
          Token<DelegationTokenIdentifier> token =
                  alluxio.security.util.KerberosUtils.getDelegationToken(subject,
                          com.google.common.net.HostAndPort
                                  .fromParts(serverInetAddress.getAddress().getHostAddress(),
                                          serverInetAddress.getPort())
                                  .toString());

          if (token != null) {
            LOG.debug("Delegation token found for subject {} and server {}: {}.", mParentSubject,
                    serverInetAddress, token);
            return ChannelAuthenticationScheme.DELEGATION_TOKEN;
          } else {
            return ChannelAuthenticationScheme.KERBEROS;
          }
          // ALLUXIO CS END
        default:
          throw new UnauthenticatedException(String.format(
                  "Configured authentication type is not supported: %s", mAuthType.getAuthName()));
      }
    }

    /**
     * Create SaslClient handler for authentication.
     *
     * @param serverAddress target server address
     * @param authScheme authentication scheme to use
     * @param subject the subject to use
     * @return the created {@link SaslClientHandler} instance
     * @throws UnauthenticatedException
     */
    private SaslClientHandler createSaslClientHandler(SocketAddress serverAddress,
        ChannelAuthenticationScheme authScheme, Subject subject) throws UnauthenticatedException {
      switch (authScheme) {
        case SIMPLE:
        case CUSTOM:
          if (mUseSubject) {
            return new alluxio.security.authentication.plain.SaslClientHandlerPlain(mParentSubject,
                    mConfiguration);
          } else {
            return new alluxio.security.authentication.plain.SaslClientHandlerPlain(mUserName,
                    mPassword, mImpersonationUser);
          }
          // ALLUXIO CS ADD
        case KERBEROS:
          Preconditions.checkArgument(serverAddress instanceof InetSocketAddress,
                  String.format("Need an Inet host for auth scheme:%s. Found: %s.", authScheme,
                          serverAddress.getClass()));
          InetSocketAddress serverInetAddress = (InetSocketAddress) serverAddress;
          return new alluxio.security.authentication.kerberos.SaslClientHandlerKerberos(null,
                  serverInetAddress, mConfiguration);
        case DELEGATION_TOKEN:
          Preconditions.checkArgument(serverAddress instanceof InetSocketAddress,
                  String.format("Need an Inet host for auth scheme:%s. Found: %s.", authScheme,
                          serverAddress.getClass()));
          InetSocketAddress serverInetAddress2 = (InetSocketAddress) serverAddress;

          alluxio.security.authentication.Token<DelegationTokenIdentifier> token =
                  alluxio.security.util.KerberosUtils.getDelegationToken(subject,
                          HostAndPort.fromParts(serverInetAddress2.getAddress().getHostAddress(),
                                  serverInetAddress2.getPort()).toString());
          return new alluxio.security.authentication.token.SaslClientHandlerToken(token,
                  serverInetAddress2);
        case CAPABILITY_TOKEN:
          Preconditions.checkArgument(serverAddress instanceof InetSocketAddress,
                  String.format("Need an Inet host for auth scheme:%s. Found: %s.", authScheme,
                          serverAddress.getClass()));
          InetSocketAddress serverInetAddress3 = (InetSocketAddress) serverAddress;

          // Extract capability token from subject
          alluxio.security.authentication.Token<?> capabilityToken =
                  alluxio.security.authentication.token.TokenUtils
                          .getCapabilityTokenFromSubject(subject);

          return new alluxio.security.authentication.token.SaslClientHandlerToken(capabilityToken,
                  serverInetAddress3);
        // ALLUXIO CS END
        default:
          throw new UnauthenticatedException(
                  String.format("Channel authentication scheme not supported: %s", authScheme.name()));
      }
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
      return mChannel.newCall(methodDescriptor, callOptions);
    }

    @Override
    public String authority() {
      return mChannel.authority();
    }

    @Override
    public boolean isAuthenticated() {
      return mAuthenticated;
    }
  }
}
