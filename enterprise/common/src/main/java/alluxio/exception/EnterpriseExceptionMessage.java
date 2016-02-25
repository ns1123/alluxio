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

package alluxio.exception;

import java.text.MessageFormat;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

/**
 * Exception messages used across Alluxio Enterprise.
 *
 * Note: To minimize merge conflicts, please sort alphabetically in each section.
 */
@ThreadSafe
public enum EnterpriseExceptionMessage {
  // job manager
  JOB_DEFINITION_DOES_NOT_EXIST("The job definition for config {0} does not exist"),
  JOB_DOES_NOT_EXIST("The job of id {0} does not exist");

  // SEMICOLON! minimize merge conflicts by putting it on its own line
  ;

  private final MessageFormat mMessage;

  EnterpriseExceptionMessage(String message) {
    mMessage = new MessageFormat(message);
  }

  /**
   * Formats the message of the exception.
   *
   * @param params the parameters for the exception message
   * @return the formatted message
   */
  public String getMessage(Object... params) {
    Preconditions.checkArgument(mMessage.getFormats().length == params.length, "The message takes "
        + mMessage.getFormats().length + " arguments, but is given " + params.length);
    // MessageFormat is not thread-safe, so guard it
    synchronized (mMessage) {
      return mMessage.format(params);
    }
  }
}
