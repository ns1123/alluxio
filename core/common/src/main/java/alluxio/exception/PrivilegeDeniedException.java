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

package alluxio.exception;

import alluxio.wire.Privilege;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Exception thrown when a privileged operation is attempted by a user lacking the privilege.
 */
@ThreadSafe
public final class PrivilegeDeniedException extends RuntimeException {
  private static final long serialVersionUID = -2574859323859591460L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public PrivilegeDeniedException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public PrivilegeDeniedException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new exception with the specified exception message and multiple parameters.
   *
   * @param message the exception message
   * @param params the parameters
   */
  public PrivilegeDeniedException(ExceptionMessage message, Object... params) {
    this(message.getMessage(params));
  }

  /**
   * Constructs a new exception with the specified exception message, the cause and multiple
   * parameters.
   *
   * @param message the exception message
   * @param cause the cause
   * @param params the parameters
   */
  public PrivilegeDeniedException(ExceptionMessage message, Throwable cause, Object... params) {
    this(message.getMessage(params), cause);
  }

  /**
   * Constructs a new exception with the an exception message explaining that the given user lacks
   * the given privilege.
   *
   * @param user the user
   * @param privilege the privilege
   */
  public PrivilegeDeniedException(String user, Privilege privilege) {
    this(ExceptionMessage.PRIVILEGE_DENIED, user, privilege);
  }
}
