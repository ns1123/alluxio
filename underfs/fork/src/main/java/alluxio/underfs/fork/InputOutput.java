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

package alluxio.underfs.fork;

/**
 * Class for passing both input and output as an argument to {@code Function}.
 *
 * @param <T> the input type
 * @param <U> the output type
 */
class InputOutput<T, U> {
  private T mInput;
  private U mOutput;

  /**
   * Creates a new instance of {@link InputOutput}.
   *
   * @param input the input to use
   * @param output the output to use
   */
  InputOutput(T input, U output) {
    mInput = input;
    mOutput = output;
  }

  /**
   * @return the input
   */
  T getInput() {
    return mInput;
  }

  /**
   * @return the output
   */
  U getOutput() {
    return mOutput;
  }
}
