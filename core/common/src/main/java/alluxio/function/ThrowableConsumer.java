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

package alluxio.function;

/**
 * A function interface which takes one argument, returns nothing, and throws exceptions.
 *
 * @param <T> the type of the argument
 */
@FunctionalInterface
public interface ThrowableConsumer<T> {
  /**
   * Performs this operation on the given arguments.
   *
   * @param t the input argument
   */
  void accept(T t) throws Exception;
}
