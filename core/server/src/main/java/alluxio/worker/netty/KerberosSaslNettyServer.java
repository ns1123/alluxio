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

import alluxio.Constants;
import alluxio.security.LoginUser;
import alluxio.security.util.KerberosName;
import alluxio.security.util.KerberosUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * A Sasl secured Netty Server, with Kerberos Login.
 */
public class KerberosSaslNettyServer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private SaslServer mSaslServer;
  private Subject mSubject;

  /**
   * Constructs a KerberosSaslNettyServer.
   *
   * @throws SaslException if failed to create a Sasl netty server
   */
  public KerberosSaslNettyServer() throws SaslException {
    KerberosName name;
    try {
      name = KerberosUtils.getServerKerberosName();
    } catch (AccessControlException e) {
      throw new SaslException("AccessControlException ", e);
    }

    try {
      mSubject = LoginUser.getServerLoginSubject();
    } catch (IOException e) {
      throw new SaslException("IOException ", e);
    }

    try {
      final String hostName = name.getHostName();
      final String serviceName = name.getServiceName();
      mSaslServer = Subject.doAs(mSubject, new PrivilegedExceptionAction<SaslServer>() {
        public SaslServer run() {
          try {
            return Sasl.createSaslServer(KerberosUtils.GSSAPI_MECHANISM_NAME, serviceName, hostName,
                KerberosUtils.SASL_PROPERTIES, new KerberosUtils.GssSaslCallbackHandler());
          } catch (Exception e) {
            LOG.error("Subject failed to create Sasl client. ", e);
            return null;
          }
        }
      });
    } catch (PrivilegedActionException e) {
      throw new SaslException("KerberosSaslNettyServer: Could not create Sasl Netty Server. ", e);
    }
  }

  /**
   * Returns whether the Sasl server is complete.
   *
   * @return true iff the Sasl server is marked as complete, false otherwise
   */
  public boolean isComplete() {
    return mSaslServer.isComplete();
  }

  /**
   * Generates the response to a SASL tokens.
   *
   * @param token Server's SASL token
   * @return token to send back to the the other side
   * @throws SaslException if failed to respond to the given token
   */
  public byte[] response(final byte[] token) throws SaslException {
    try {
      return Subject.doAs(mSubject, new PrivilegedExceptionAction<byte[]>() {
        public byte[] run() throws  SaslException {
          return mSaslServer.evaluateResponse(token);
        }
      });
    } catch (PrivilegedActionException e) {
      throw new SaslException("Failed to generate response for token. ", e);
    }
  }
}

