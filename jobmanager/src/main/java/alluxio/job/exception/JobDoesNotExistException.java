/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.exception;

import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The exception thrown when a job definition does not exist in Alluxio.
 */
@ThreadSafe
public class JobDoesNotExistException extends AlluxioException {
  private static final long serialVersionUID = -7291730624984048562L;

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   */
  public JobDoesNotExistException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause
   */
  public JobDoesNotExistException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new exception with the specified exception message and multiple parameters.
   *
   * @param message the exception message
   * @param params the parameters
   */
  public JobDoesNotExistException(ExceptionMessage message, Object... params) {
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
  public JobDoesNotExistException(ExceptionMessage message, Throwable cause, Object... params) {
    this(message.getMessage(params), cause);
  }

  /**
   * Constructs a new exception saying the specified job ID does not exist.
   *
   * @param jobId the job id which does not exit
   */
  public JobDoesNotExistException(long jobId) {
    this(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(jobId));
  }
}
