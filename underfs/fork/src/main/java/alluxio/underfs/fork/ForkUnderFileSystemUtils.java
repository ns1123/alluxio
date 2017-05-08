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

import com.google.common.base.Function;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility methods for {@link ForkUnderFileSystem}.
 */
public final class ForkUnderFileSystemUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ForkUnderFileSystemUtils.class);

  /**
   * Invokes the given function over all inputs.
   *
   * @param function the function to invoke
   * @param inputs the collection of inputs
   * @param <T> the input type
   * @throws IOException if any of the invocations throws IOException
   */
  static <T> void invokeAll(Function<T, IOException> function, Collection<T> inputs)
      throws IOException {
    List<IOException> exceptions = new ArrayList<>();
    for (T input : inputs) {
      IOException e = function.apply(input);
      if (e != null) {
        exceptions.add(e);
      }
    }
    if (!exceptions.isEmpty()) {
      for (IOException e : exceptions) {
        LOG.warn("invocation failed: {}", e.getMessage());
        LOG.debug("Exception:", e);
      }
      throw new IOException(exceptions.get(0));
    }
  }

  /**
   * Invokes the given function over all inputs, collecting the outputs.
   *
   * @param function the function to invoke
   * @param inputs the list of inputs
   * @param output the output reference
   * @param <T> the input type
   * @param <U> the output type
   * @throws IOException if any of the invocations throws IOException
   */
  static <T, U> void invokeAll(Function<Pair<T, U>, IOException> function,
      Collection<T> inputs, U output) throws IOException {
    List<IOException> exceptions = new ArrayList<>();
    for (T input : inputs) {
      Pair<T, U> arg = new ImmutablePair<>(input, output);
      IOException e = function.apply(arg);
      if (e != null) {
        exceptions.add(e);
      }
    }
    if (!exceptions.isEmpty()) {
      for (IOException e : exceptions) {
        LOG.warn("invocation failed: {}", e.getMessage());
        LOG.debug("Exception:", e);
      }
      throw new IOException(exceptions.get(0));
    }
  }

  /**
   * Invokes the given function over the given inputs one by one, until an invocation succeeds.
   *
   * @param function the function to invoke
   * @param inputs the list of inputs
   * @param <T> the input type
   * @throws IOException if all of the invocations fail
   */
  static <T> void invokeOne(Function<T, IOException> function, Collection<T> inputs)
      throws IOException {
    List<IOException> exceptions = new ArrayList<>();
    for (T input : inputs) {
      IOException e = function.apply(input);
      if (e == null) {
        return;
      }
      exceptions.add(e);
    }
    if (!exceptions.isEmpty()) {
      for (IOException e : exceptions) {
        LOG.warn("invocation failed: {}", e.getMessage());
        LOG.debug("Exception:", e);
      }
      throw new IOException(exceptions.get(0));
    }
  }

  /**
   * Invokes the given function over the given inputs one by one, until an invocation succeeds,
   * returning its result.
   *
   * @param function the function to invoke
   * @param inputs the list of inputs
   * @param output the output reference
   * @param <T> the input type
   * @param <U> the output type
   * @throws IOException if all of the invocations fail
   */
  static <T, U> void invokeOne(Function<Pair<T, U>, IOException> function,
      Collection<T> inputs, U output) throws IOException {
    List<IOException> exceptions = new ArrayList<>();
    for (T input : inputs) {
      Pair<T, U> arg = new ImmutablePair<>(input, output);
      IOException e = function.apply(arg);
      if (e == null) {
        return;
      }
      exceptions.add(e);
    }
    if (!exceptions.isEmpty()) {
      for (IOException e : exceptions) {
        LOG.warn("invocation failed: {}", e.getMessage());
        LOG.debug("Exception:", e);
      }
      throw new IOException(exceptions.get(0));
    }
  }

  private ForkUnderFileSystemUtils() {} // prevent instantiation
}
