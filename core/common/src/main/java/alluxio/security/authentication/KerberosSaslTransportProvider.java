/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.security.LoginUser;

import com.google.common.base.Preconditions;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;

/**
 * SASL transport provider when authentication type is {@link AuthType#KERBEROS).
 */
@ThreadSafe
public final class KerberosSaslTransportProvider implements TransportProvider {
  private static final String GSSAPI_MECHANISM_NAME = "GSSAPI";
  /** Timeout for socket in ms. */
  private int mSocketTimeoutMs;
  /** Configuration. */
  private Configuration mConfiguration;

  /**
   * CallbackHandler for SASL GSSAPI Kerberos mechanism.
   */
  public static class SaslGssCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws
        UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL GSSAPI Callback");
        }
      }

      if (ac != null) {
        String authenticationId = ac.getAuthenticationID();
        String authorizationId = ac.getAuthorizationID();
        if (authenticationId.equals(authorizationId)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          ac.setAuthorizedID(authorizationId);
          // After verification succeeds, a user with this authorizationId will be set to a
          // Threadlocal.
          AuthenticatedClientUser.set(authorizationId);
        }
      }
    }
  }

  /**
   * Constructor for transport provider when authentication type is {@link AuthType#KERBEROS).
   *
   * @param conf Alluxio configuration
   */
  public KerberosSaslTransportProvider(Configuration conf) {
    mConfiguration = Preconditions.checkNotNull(conf);
    mSocketTimeoutMs = conf.getInt(Constants.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS);
  }

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress) throws IOException {
    String[] names = parseServerKerberosPrincipal();
    if (names.length < 2) {
      throw new IOException("Invalid Kerberos principal: " + String.join(" ", names));
    }
    Subject subject = LoginUser.getClientLoginSubject(mConfiguration);

    try {
      return getClientTransport(subject, names[0], names[1], serverAddress);
    } catch (PrivilegedActionException e) {
      throw new IOException("PrivilegedActionException" + e);
    }
  }

  /**
   * Gets a client thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serviceName Thrift SASL service name
   * @param serverAddress thrift server address
   * @return Thrift transport
   * @throws SaslException when it failed to create a Thrift transport
   * @throws PrivilegedActionException when the Subject doAs failed
   */
  public TTransport getClientTransport(Subject subject,
                                       final String protocol,
                                       final String serviceName,
                                       final InetSocketAddress serverAddress)
      throws SaslException, PrivilegedActionException {
    return Subject.doAs(subject, new
          PrivilegedExceptionAction<TSaslClientTransport>() {
        public TSaslClientTransport run() throws AuthenticationException {
          try {
            TTransport wrappedTransport =
                TransportProviderUtils.createThriftSocket(serverAddress, mSocketTimeoutMs);
            Map<String, String> saslProperties = new HashMap<String, String>();
            saslProperties.put(Sasl.QOP, "auth");
            return new TSaslClientTransport(
                GSSAPI_MECHANISM_NAME, null /* authorizationId */,
                protocol, serviceName, saslProperties, null, wrappedTransport);
          } catch (SaslException e) {
            throw new AuthenticationException("Exception initializing SASL client", e);
          }
        }
      });
  }

  @Override
  public TTransportFactory getServerTransportFactory() throws SaslException {
    String[] names = parseServerKerberosPrincipal();
    if (names.length < 2) {
      throw new SaslException("Invalid Kerberos principal: " + String.join(" ", names));
    }

    try {
      Subject subject = LoginUser.getServerLoginSubject(mConfiguration);
      return getServerTransportFactory(subject, names[0], names[1]);
    } catch (PrivilegedActionException e) {
      throw new SaslException("PrivilegedActionException" + e);
    } catch (IOException e) {
      throw new SaslException("IOException" + e);
    }
  }

  /**
   * Gets a server thrift transport with Kerberos login subject.
   *
   * @param subject Kerberos subject
   * @param protocol Thrift SASL protocol name
   * @param serviceName Thrift SASL service name
   * @return a server transport
   * @throws SaslException when SASL can't be initialized
   * @throws PrivilegedActionException when the Subject doAs failed
   */
  public TTransportFactory getServerTransportFactory(
      Subject subject, final String protocol, final String serviceName)
      throws SaslException, PrivilegedActionException {
    final Map<String, String> saslProperties = new HashMap<String, String>();
    saslProperties.put(Sasl.QOP, "auth");

    return Subject.doAs(subject, new PrivilegedExceptionAction<TSaslServerTransport.Factory>() {
        public TSaslServerTransport.Factory run() {
            TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
            saslTransportFactory.addServerDefinition(
                GSSAPI_MECHANISM_NAME, protocol, serviceName, saslProperties,
                new SaslGssCallbackHandler());
            return saslTransportFactory;
        }
      });
  }

  /**
   * Parses a server Kerberos principal, which is stored in
   * {@link Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL}.
   *
   * @return a list of strings representing three parts: the primary, the instance, and the realm
   * @throws AccessControlException if server principal config is invalid
   * @throws SaslException if server principal config is not specified
   */
  private String[] parseServerKerberosPrincipal() throws AccessControlException, SaslException {
    if (!mConfiguration.containsKey(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL)) {
      throw new SaslException("Failed to parse server principal: "
          + Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL + " must be set.");
    }
    String principal = mConfiguration.get(Constants.SECURITY_KERBEROS_SERVER_PRINCIPAL);
    final String[] names = principal.split("[/@]");
    // Realm can be non-specified and default from system config, so names should contain at least
    // 2 parts: primary name and instance name.
    if (names.length < 2) {
      throw new AccessControlException(
          "Kerberos server principal name does NOT have the expected hostname part: " + principal);
    }
    return names;
  }
}
