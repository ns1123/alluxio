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
  static <T> void invokeAll(final Function<T, IOException> function, Collection<T> inputs)
      throws IOException {
    final List<IOException> exceptions = apply(function, inputs);
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
   * Invokes the given function over all inputs, succeeding as long as at least one invocation
   * succeeds.
   *
   * @param function the function to invoke
   * @param inputs the collection of inputs
   * @param <T> the input type
   * @throws IOException if all of the invocations throw IOException
   */
  static <T> void invokeSome(Function<T, IOException> function, Collection<T> inputs)
      throws IOException {
    final List<IOException> exceptions = apply(function, inputs);
    if (!exceptions.isEmpty()) {
      for (IOException e : exceptions) {
        LOG.warn("invocation failed: {}", e.getMessage());
        LOG.debug("Exception:", e);
      }
      if (exceptions.size() == inputs.size()) {
        throw new IOException(exceptions.get(0));
      }
    }
  }

  /**
   * Applies the given function to all inputs.
   *
   * @param function the function to apply
   * @param inputs the inputs to use
   * @param <T> the input type
   * @return the collection of {@link IOException}s that occurred
   */
  private static <T> List<IOException> apply(final Function<T, IOException> function,
      Collection<T> inputs) {
    final List<IOException> exceptions = new ArrayList<>();
    final List<Thread> threads = new ArrayList<>();
    for (final T input : inputs) {
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          IOException e = function.apply(input);
          if (e != null) {
            exceptions.add(e);
          }
        }
      });
      t.start();
      threads.add(t);
    }
    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return exceptions;
  }

  /**
   * Folds the given value into the given list.
   *
   * @param list the list to use
   * @param value the value to use
   * @param <T> the type of the list elements
   * @param <U> the type of the value
   * @return the folded list
   */
  static <T, U> Collection<Pair<T, U>> fold(Collection<T> list, U value) {
    List<Pair<T, U>> foldedList = new ArrayList<>();
    for (T element : list) {
      foldedList.add(new ImmutablePair<>(element, value));
    }
    return foldedList;
  }

  private ForkUnderFileSystemUtils() {} // prevent instantiation
}
