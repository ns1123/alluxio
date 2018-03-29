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

import com.google.common.base.Preconditions;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;

/**
 * Provides Kerberos-aware thrift protocol.
 */
public final class AuthenticatedThriftProtocol extends DelegatingTProtocol {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticatedThriftProtocol.class);

  /**
   * @param protocol TMultiplexedProtocol to wrap
   * @param subject the subject to authenticate as
   */
  public AuthenticatedThriftProtocol(TProtocol protocol, Subject subject) {
    super(protocol, new KerberosTTransport(protocol.getTransport(),
        Preconditions.checkNotNull(subject, "subject")));
  }

  /**
   * Wrapper around a transport which performs the open and close methods as a specific subject.
   */
  private static class KerberosTTransport extends DelegatingTTransport {
    private Subject mSubject;
    private TTransport mTransport;

    /**
     * @param transport the transport to delegate to
     */
    public KerberosTTransport(TTransport transport, Subject subject) {
      super(transport);
      mTransport = transport;
      mSubject = subject;
    }
<<<<<<< HEAD
||||||| merged common ancestors
    try {
      Subject.doAs(mSubject,
          new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
              transport.open();
              return null;
            }
          });
    } catch (PrivilegedActionException e) {
      throw new TTransportException("Failed to open Kerberos transport" , e);
    }
  }
=======
    try {
      Subject.doAs(mSubject,
          new PrivilegedExceptionAction<Void>() {
            public Void run() throws TTransportException {
              transport.open();
              return null;
            }
          });
    } catch (PrivilegedActionException | IllegalStateException e) {
      throw new TTransportException("Failed to open Kerberos transport", e);
    }
  }
>>>>>>> enterprise-1.7

    @Override
    public void open() throws TTransportException {
      try {
        Subject.doAs(mSubject, (PrivilegedExceptionAction<Void>) () -> {
          mTransport.open();
          return null;
        });
      } catch (PrivilegedActionException e) {
        throw new TTransportException("Failed to open Kerberos transport", e);
      }
    }

    @Override
    public void close() {
      try {
        Subject.doAs(mSubject, (PrivilegedExceptionAction<Void>) () -> {
          mTransport.close();
          return null;
        });
      } catch (PrivilegedActionException e) {
        LOG.error("Failed to close Kerberos transport", e);
      }
    }
  }
}

