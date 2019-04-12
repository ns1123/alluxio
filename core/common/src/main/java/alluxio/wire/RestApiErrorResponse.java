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

package alluxio.wire;

import alluxio.exception.status.Status;

import com.google.common.base.Objects;

/**
 * Error response when REST API throws an exception.
 * It will be encoded into a Json string to be returned as an error response for the REST call.
 */
public final class RestApiErrorResponse {
  private Status mStatus;
  private String mMessage;

  /**
   * Creates an instance with empty status and message.
   */
  public RestApiErrorResponse() {
  }

  /**
   * Creates a new {@link RestApiErrorResponse}.
   *
   * @param status  the RPC call result's {@link Status}
   * @param message the error message
   */
  public RestApiErrorResponse(Status status, String message) {
    mStatus = status;
    mMessage = message;
  }

  /**
   * @return the status
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @return the error message
   */
  public String getMessage() {
    return mMessage;
  }
  /**
   * @param status the status code
   */
  public void setStatus(Status status) {
    mStatus = status;
  }

  /**
   * @param message the error message
   */
  public void setMessage(String message) {
    mMessage = message;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .addValue(mStatus)
        .addValue(mMessage).toString();
  }
}
