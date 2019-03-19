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
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Forwarder for {@link SaslClient} instances to run under privileged context.
 */
public class PrivilegedSaslClient implements SaslClient {
  /** Underlying @{link SaslClient}. */
  private final SaslClient mSaslClient;

  /** User subject under which to invoke underlying SaslClient. */
  private final Subject mSubject;

  /**
   * @param saslClient {@link SaslClient} to wrap
   * @param subject user context to run given {@link SaslClient}
   */
  public PrivilegedSaslClient(SaslClient saslClient, Subject subject) {
    mSaslClient = saslClient;
    mSubject = subject;
  }

  @Override
  public String getMechanismName() {
    return mSaslClient.getMechanismName();
  }

  @Override
  public boolean hasInitialResponse() {
    return mSaslClient.hasInitialResponse();
  }

  @Override
  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    try {
      return Subject.doAs(mSubject, new PrivilegedExceptionAction<byte[]>() {
        @Override
        public byte[] run() throws Exception {
          return mSaslClient.evaluateChallenge(challenge);
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
    return mSaslClient.isComplete();
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
    return mSaslClient.unwrap(incoming, offset, len);
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
    return mSaslClient.wrap(outgoing, offset, len);
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    return mSaslClient.getNegotiatedProperty(propName);
  }

  @Override
  public void dispose() throws SaslException {
    mSaslClient.dispose();
  }
}
