/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.security.authentication;

import java.security.Provider;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The Java SunSASL provider supports CRAM-MD5, DIGEST-MD5 and GSSAPI mechanisms on the server side.
 * When the SASL is using PLAIN mechanism, there is no support the SASL server. So there is a new
 * provider needed to register to support server-side PLAIN mechanism.
 * <p/>
 * Three basic steps to complete a SASL security provider:
 * <ol>
 * <li>Implements {@link PlainSaslServer} class which extends {@link javax.security.sasl.SaslServer}
 * interface</li>
 * <li>Provides {@link PlainSaslServer.Factory} class that implements
 * {@link javax.security.sasl.SaslServerFactory} interface</li>
 * <li>Provides a JCA provider that registers the factory</li>
 * </ol>
 */
@ThreadSafe
public final class PlainSaslProvider extends Provider {
  private static final long serialVersionUID = 4583558117355348638L;

  public static final String NAME = "PlainSasl";
  public static final String MECHANISM = "PLAIN";
  public static final double VERSION = 1.0;

  /**
   * Constructs a new provider for the SASL server when using the PLAIN mechanism.
   */
  public PlainSaslProvider() {
    super(NAME, VERSION, "Plain SASL server provider");
    put("SaslServerFactory." + MECHANISM, PlainSaslServer.Factory.class.getName());
  }

}
