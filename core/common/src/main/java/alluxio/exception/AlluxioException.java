/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.exception;

import alluxio.thrift.AlluxioTException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * General {@link AlluxioException} used throughout the system. It must be able serialize itself to
 * the RPC framework and convert back without losing any necessary information.
 */
@ThreadSafe
public abstract class AlluxioException extends Exception {
  private static final long serialVersionUID = 2243833925609642384L;

  /**
   * Constructs a {@link AlluxioException} with an exception type from a {@link AlluxioTException}.
   *
   * @param te the type of the exception
   */
  protected AlluxioException(AlluxioTException te) {
    super(te.getMessage());
  }

<<<<<<< HEAD
  protected AlluxioException(Throwable cause) {
||||||| merged common ancestors
  protected AlluxioException(AlluxioExceptionType type, Throwable cause) {
=======
  /**
   * Constructs an {@link AlluxioException} with the given cause.
   *
   * @param cause the cause
   */
  protected AlluxioException(Throwable cause) {
>>>>>>> OPENSOURCE/master
    super(cause);
  }

<<<<<<< HEAD
  protected AlluxioException(String message) {
||||||| merged common ancestors
  protected AlluxioException(AlluxioExceptionType type, String message) {
=======
  /**
   * Constructs an {@link AlluxioException} with the given message.
   *
   * @param message the message
   */
  protected AlluxioException(String message) {
>>>>>>> OPENSOURCE/master
    super(message);
  }

<<<<<<< HEAD
  protected AlluxioException(String message, Throwable cause) {
||||||| merged common ancestors
  protected AlluxioException(AlluxioExceptionType type, String message, Throwable cause) {
=======
  /**
   * Constructs an {@link AlluxioException} with the given message and cause.
   *
   * @param message the message
   * @param cause the cause
   */
  protected AlluxioException(String message, Throwable cause) {
>>>>>>> OPENSOURCE/master
    super(message, cause);
  }

  /**
   * Constructs a {@link AlluxioTException} from a {@link AlluxioException}.
   *
   * @return a {@link AlluxioTException} of the type of this exception
   */
  public AlluxioTException toAlluxioTException() {
    return new AlluxioTException(getClass().getName(), getMessage());
  }

  /**
   * Constructs a {@link AlluxioException} from a {@link AlluxioTException}.
   *
   * @param e the {link AlluxioTException} to convert to a {@link AlluxioException}
   * @return a {@link AlluxioException} of the type specified in e, with the message specified in e
   */
  @SuppressWarnings("unchecked")
  public static AlluxioException from(AlluxioTException e) {
<<<<<<< HEAD
    Class<? extends AlluxioException> throwClass;
||||||| merged common ancestors
    AlluxioExceptionType exceptionType = AlluxioExceptionType.valueOf(e.getType());
    Class<? extends AlluxioException> throwClass = exceptionType.getExceptionClass();
=======
>>>>>>> OPENSOURCE/master
    try {
<<<<<<< HEAD
      throwClass = (Class<? extends AlluxioException>) Class.forName(e.getType());
      AlluxioException throwInstance =
          throwClass.getConstructor(String.class).newInstance(e.getMessage());
      return throwInstance;
||||||| merged common ancestors
      AlluxioException throwInstance =
          throwClass.getConstructor(String.class).newInstance(e.getMessage());
      return throwInstance;
=======
      @SuppressWarnings("unchecked")
      Class<? extends AlluxioException> throwClass =
          (Class<? extends AlluxioException>) Class.forName(e.getType());
      return throwClass.getConstructor(String.class).newInstance(e.getMessage());
>>>>>>> OPENSOURCE/master
    } catch (ReflectiveOperationException reflectException) {
      String errorMessage = "Could not instantiate " + e.getType() + " with a String-only "
          + "constructor: " + reflectException.getMessage();
      throw new IllegalStateException(errorMessage, reflectException);
    }
  }
}
