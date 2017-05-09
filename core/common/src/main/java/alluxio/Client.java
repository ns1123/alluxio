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

package alluxio;

import java.net.InetSocketAddress;

/**
 * Interface for a client in the Alluxio system.
 */
public interface Client extends QuietlyCloseable {

  /**
   * Connects with the remote.
   */
  void connect();

  /**
   * Closes the connection with the Alluxio remote and does the necessary cleanup. It should be used
   * if the client has not connected with the remote for a while, for example.
   */
  void disconnect();

  /**
   * @return the {@link InetSocketAddress} of the remote
   */
  InetSocketAddress getAddress();

  /**
   * Returns the connected status of the client.
   *
   * @return true if this client is connected to the remote
   */
  boolean isConnected();

  /**
   * Closes the connection, then queries and sets current remote address.
   */
  void resetConnection();
}
