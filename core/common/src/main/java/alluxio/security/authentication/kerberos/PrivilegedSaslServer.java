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

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Forwarder for {@link SaslServer} instances to run under privileged context.
 */
public class PrivilegedSaslServer implements SaslServer {

  /** Underlying @{link SaslServer}. */
  private final SaslServer mSaslServer;

  /** User subject under which to invoke underlying SaslServer. */
  private final Subject mSubject;

  /**
   * @param saslServer {@link SaslServer} to wrap
   * @param subject user context to run given {@link SaslServer}
   */
  public PrivilegedSaslServer(SaslServer saslServer, Subject subject) {
    mSaslServer = saslServer;
    mSubject = subject;
  }

  @Override
  public String getMechanismName() {
    return mSaslServer.getMechanismName();
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    try {
      return Subject.doAs(mSubject, new PrivilegedExceptionAction<byte[]>() {
        @Override
        public byte[] run() throws SaslException {
          return mSaslServer.evaluateResponse(response);
        }
      });
    } catch (PrivilegedActionException pe) {
      Throwable cause = pe.getCause();
      if (cause != null && cause instanceof SaslException) {
        throw (SaslException) cause;
      }
      throw new RuntimeException((cause != null) ? cause : pe);
    }
  }

  @Override
  public boolean isComplete() {
    return mSaslServer.isComplete();
  }

  @Override
  public String getAuthorizationID() {
    return mSaslServer.getAuthorizationID();
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    return mSaslServer.unwrap(incoming, offset, len);
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    return mSaslServer.wrap(outgoing, offset, len);
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    return mSaslServer.getNegotiatedProperty(propName);
  }

  @Override
  public void dispose() throws SaslException {
    mSaslServer.dispose();
  }
}
