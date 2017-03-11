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

package alluxio.netty;

import io.netty.util.AttributeKey;

/**
 * This class defines some netty attribute keys.
 */
public final class NettyAttributes {
  /** The key for the user name in the netty channel. */
  public static final AttributeKey<String> CHANNEL_KERBEROS_USER_KEY =
      AttributeKey.valueOf("CHANNEL_KERBEROS_USER_KEY");

  /** The key to indicate whether the channel has been registered to block worker or not. */
  public static final io.netty.util.AttributeKey<Boolean> CHANNEL_REGISTERED_TO_BLOCK_WORKER =
      io.netty.util.AttributeKey.valueOf("CHANNEL_REGISTERED_TO_BLOCK_WORKER");

  /** The key for the remote hostname in the netty bootstrap and channel. */
  public static final AttributeKey<String> HOSTNAME_KEY = AttributeKey.valueOf("REMOTE_HOSTNAME");

  /**
   * Private constructor.
   */
  private NettyAttributes() {}
}
