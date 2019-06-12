/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.xattr.ExtendedAttribute;
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Utility methods for {@link UnionUnderFileSystem}.
 */
public final class UnionUnderFileSystemUtils {
  private static final Logger LOG = LoggerFactory.getLogger(UnionUnderFileSystemUtils.class);

  @FunctionalInterface
  interface ConsumeThree<T, R, K> {
    void apply(T t, R r, K k) throws IOException;
  }

  /**
   * Invokes a consumer function across the given inputs, with the option to return early if the
   * first set of invocations meets the specified user's condition.
   *
   * @param invokeFunc the function to invoke. For example,
   *                   {@link #invokeOne(UncheckedIOConsumer, Collection)},
   *                   {@link #invokeAll(ExecutorService, UncheckedIOConsumer, Collection)}, etc
   * @param service the executor service used to process operations in parallel
   * @param consumer the function which will execute in parallel for all inputs
   * @param continueFunc this function returns true or false if the invocation should continue on
   *                     the second set of the inputs
   * @param inputs       a pair of inputs. The right half of the pair of the collection will only
   *                     be executed
   * @param <T> The type of the input to the function
   * @throws IOException if the second set of inputs results in an IOException and the
   *                     continueFunc has not been satisfied
   */
  static <T> void splitInvoke(
      ConsumeThree<ExecutorService, UncheckedIOConsumer<T>, Collection<T>> invokeFunc,
      ExecutorService service, final UncheckedIOConsumer<T> consumer,
      Supplier<Boolean> continueFunc, Pair<Collection<T>, Collection<T>> inputs)
      throws IOException {
    // First, try to invoke the left hand side of the pair
    // The left hand side should contain the UFSes to attempt first
    // If this call succeeds, then we simply return.
    List<IOException> exceptions = new ArrayList<>(2); // We'll only ever get 2 exceptions
    boolean cont;
    try {
      invokeFunc.apply(service, consumer, inputs.getLeft());
    } catch (IOException e) {
      exceptions.add(e);
      LOG.debug("First set of UFS calls threw IOException: {}", e.getMessage());
    } finally {
      cont = continueFunc.get();
    }
    if (cont) {
      try {
        invokeFunc.apply(service, consumer, inputs.getRight());
      } catch (IOException e) {
        exceptions.add(e);
        throw new AggregateException(exceptions);
      }
    }
    // If the continueFunc is still true, both invokes didn't satisfy the function. Throw and
    // propagate any errors
    if (continueFunc.get()) {
      exceptions.add(new IOException("both sets of invocation inputs did not result in "
          + "the continuation function being satisfied"));
      throw new AggregateException(exceptions);
    }
  }

  /**
   * Invokes the given function concurrently over the given inputs, succeeding if all invocations
   * succeed.
   *
   * @param service the executor service to use
   * @param function the function to invoke
   * @param inputs the collection of inputs
   * @param <T> the input type
   * @throws IOException if any of the invocations throws IOException
   */
  static <T> void invokeAll(ExecutorService service, final UncheckedIOConsumer<T> function,
      Collection<T> inputs) throws IOException {
    invokeAll(service, function, inputs, Collections.emptySet());
  }

  /**
   * Invokes the given function concurrently over the given inputs, succeeding if all invocations
   * succeed. If the exception thrown by an invocation is found in the {@code ignoreExceptionNames}
   * parameter, the exception will be ignored, and the invocation will be considered successful.
   *
   * @param service the executor service to use
   * @param function the function to invoke
   * @param inputs the collection of inputs
   * @param ignoreExceptionNames the set of exception {@link Class} to ignore
   * @param <T> the input type
   * @throws IOException if any of the invocations throws IOException
   */
  static <T> void invokeAll(ExecutorService service, final UncheckedIOConsumer<T> function,
      Collection<T> inputs, Set<String> ignoreExceptionNames) throws IOException {
    List<IOException> exceptions = apply(service, function, inputs);
    final List<IOException> filteredExceptions =
        exceptions.stream().filter(e -> !ignoreExceptionNames.contains(e.getClass().getName()))
            .collect(Collectors.toList());

    if (!filteredExceptions.isEmpty()) {
      for (IOException e : filteredExceptions) {
        LOG.warn("invocation failed: {}", e.getMessage());
        LOG.debug("Exception:", e);
      }
      throw new AggregateException(filteredExceptions);
    }
  }

  /**
   * Invokes the given function sequentially over the given inputs, until an invocation succeeds.
   *
   * @param function the function to invoke
   * @param inputs the list of inputs
   * @param <T> the input type
   * @throws IOException if all of the invocations fail
   */
  static <T> void invokeOne(UncheckedIOConsumer<T> function, Collection<T> inputs)
      throws IOException {
    List<IOException> exceptions = new ArrayList<>();
    for (T input : inputs) {
      try {
        function.apply(input);
        return;
      } catch (IOException e) {
        exceptions.add(e);
      }
    }
    if (!exceptions.isEmpty()) {
      for (IOException e : exceptions) {
        LOG.warn("invocation failed: {}", e.getMessage());
        LOG.debug("Exception:", e);
      }
      throw new AggregateException(exceptions);
    }
  }

  /**
   * Invoke a function sequentially over a series of inputs, stopping if the condition of the
   * continue function is not met.
   *
   * @param function the function to execute over the inputs
   * @param continueFunc returns true if we should continue processing, false otherwise
   * @param inputs the inputs to process
   * @param <T>
   */
  static <T> List<IOException> invokeSequentially(UncheckedIOConsumer<T> function,
      Supplier<Boolean> continueFunc, Collection<T> inputs) {
    List<IOException> exceptions = new ArrayList<>();
    for (T input : inputs) {
      try {
        function.apply(input);
      } catch (IOException e) {
        exceptions.add(e);
      }
      if (!continueFunc.get()) {
        return exceptions;
      }
    }
    return exceptions;
  }

  /**
   * Invokes the given function concurrently over the given inputs, succeeding if at least one
   * invocation succeeds.
   *
   * @param service the executor service to use
   * @param function the function to invoke
   * @param inputs the collection of inputs
   * @param <T> the input type
   * @throws IOException if all of the invocations throw IOException
   */
  static <T> void invokeSome(ExecutorService service, UncheckedIOConsumer<T> function,
      Collection<T> inputs) throws IOException {
    final List<IOException> exceptions = apply(service, function, inputs);
    if (!exceptions.isEmpty()) {
      for (IOException e : exceptions) {
        LOG.warn("invocation failed: {}", e.getMessage());
        LOG.debug("Exception:", e);
      }
      if (exceptions.size() == inputs.size()) {
        throw new AggregateException(exceptions);
      }
    }
  }

  /**
   * Invokes the given function concurrently over the given inputs.
   *
   * @param service the executor service to use
   * @param function the function to apply
   * @param inputs the inputs to use
   * @param <T> the input type
   * @return the collection of {@link IOException}s that occurred
   */
  private static <T> List<IOException> apply(ExecutorService service,
      final UncheckedIOConsumer<T> function, Collection<T> inputs) {
    final User parentUser = AuthenticatedClientUser.getOrNull();
    final List<IOException> exceptions = new ArrayList<>();
    final List<Callable<IOException>> callables = new ArrayList<>();
    for (final T input : inputs) {
      callables.add(() -> {
        final User originalUser = AuthenticatedClientUser.getOrNull();
        if (parentUser != null) {
          AuthenticatedClientUser.set(parentUser);
        }
        try {
          function.apply(input);
          return null;
        } catch (IOException e) {
          return e;
        } finally {
          if (originalUser != null) {
            AuthenticatedClientUser.set(originalUser);
          } else {
            AuthenticatedClientUser.remove();
          }
        }
      });
    }
    try {
      List<Future<IOException>> results = service.invokeAll(callables);
      for (Future<IOException> result : results) {
        IOException e = result.get();
        if (e != null) {
          exceptions.add(e);
        }
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    return exceptions;
  }

  /**
   * Takes the map of inputs to UFS status, and returns a new {@link UfsStatus} that contains
   * extended attributes which detail the persistence state of each Sub UFS in the Union.
   *
   * The information in the returned {@link UfsStatus} will mimic the information from the
   * UfsStatus from the UFS with the highest priority. Other extended attributes that may exist
   * on the highest-priority status will be preserved. Attributes on lower-priority UFS statuses
   * will *not* be preserved.
   *
   * @param inputs A map from {@link UfsKey} to {@link UfsStatus}
   * @return the highest priority {@link UfsStatus} with the extended attributes for persistence
   */
  @Nullable
  static UfsStatus mergeUfsStatusXAttr(SortedMap<UfsKey, UfsStatus> inputs) {
    if (inputs.size() == 0) {
      return null;
    }
    UfsStatus hStat = inputs.get(inputs.firstKey());
    return hStat.withXAttr(populateUfsStatusPersistenceState(hStat, inputs.keySet()));
  }

  @Nullable
  static UfsFileStatus mergeUfsFileStatusXAttr(SortedMap<UfsKey, UfsFileStatus> inputs) {
    if (inputs.size() == 0) {
      return null;
    }
    UfsFileStatus hStat = inputs.get(inputs.firstKey());
    return hStat.withXAttr(populateUfsStatusPersistenceState(hStat, inputs.keySet()));
  }

  @Nullable
  static UfsDirectoryStatus mergeUfsDirStatusXAttr(SortedMap<UfsKey, UfsDirectoryStatus> inputs) {
    if (inputs.size() == 0) {
      return null;
    }
    UfsDirectoryStatus hStat = inputs.get(inputs.firstKey());
    return hStat.withXAttr(populateUfsStatusPersistenceState(hStat, inputs.keySet()));
  }

  /**
   * Takes an input UfsStatus, and returns a new UfsStatus instance with the extended attributes
   * populated with the {@link alluxio.master.file.meta.xattr.PersistenceStateAttribute} for each
   * {@link UfsKey}
   *
   * If the existing xAttr map is null, the new UfsStatus will be instantiated with a fresh map
   * including the persistence state.
   *
   * For example, if the input set contains A, B, and C {@link UfsKey} then the resulting map
   * would look like the following (values will be encoded as {@code byte[]} of course)
   * <pre>
   *   {
   *     "system.ps.A" : "PERSISTED",
   *     "system.ps.B" : "PERSISTED",
   *     "system.ps.C" : "PERSISTED"
   *   }
   * </pre>
   *
   * @param stat The base status to add extended attributes to
   * @param inputs the set of UFSes that have the current status persisted
   * @param <T> The implementation of {@link UfsStatus}
   * @return The extended attributes that should be added to {@code stat}
   */
  @Nullable
  private static <T extends UfsStatus> Map<String, byte[]> populateUfsStatusPersistenceState(T stat,
      Set<UfsKey> inputs) {
    Map<String, byte[]> persistenceState = stat.getXAttr() != null
        ? stat.getXAttr() : new HashMap<>(inputs.size()); // don't take more mem than necessary
    inputs.forEach(key -> persistenceState.put(
        ExtendedAttribute.PERSISTENCE_STATE.forId(key.getAlias()),
        ExtendedAttribute.PERSISTENCE_STATE.encode(PersistenceState.PERSISTED)));
    return persistenceState;
  }

  @Nullable
  static UfsStatus[] mergeListStatusXAttr(SortedMap<UfsKey, UfsStatus[]> inputs) {
    // Map based on the {@link UfsStatus#getName()}
    Map<String, List<UfsKey>> ufsSet = new HashMap<>();
    // Index on {@link UfsStatus#getName()} to the status object.
    Map<String, UfsStatus> statusesSet = new HashMap<>();
    // Iterating on a sorted map - means that any UFS statuses will be added in order of priority.
    for (Map.Entry<UfsKey, UfsStatus[]> e : inputs.entrySet()) {
      UfsStatus[] statuses = e.getValue();
      if (statuses == null) {
        continue; // UFS listStatus is a @Nullable function, so we'll skip over it if null.
      }
      for (UfsStatus status : statuses) {
        if (!ufsSet.containsKey(status.getName())) {
          ufsSet.put(status.getName(), new ArrayList<>());
          // Only the status of from the UFS with the highest priority will get put into this set.
          // It relies on the inputs.keySet() to be sorted
          statusesSet.put(status.getName(), status);
        }
        ufsSet.get(status.getName()).add(e.getKey());
      }
    }

    // If no statuses to return
    if (statusesSet.size() == 0) {
      return null;
    }

    UfsStatus[] statuses = new UfsStatus[statusesSet.size()];
    int ctr = 0;
    for (Map.Entry<String, UfsStatus> entry : statusesSet.entrySet()) {
      Map<String, byte[]> xattr = entry.getValue().getXAttr() == null ? new HashMap<>() :
          entry.getValue().getXAttr();
      List<UfsKey> keys = ufsSet.get(entry.getKey());
      for (UfsKey k : keys) {
        xattr.put(ExtendedAttribute.PERSISTENCE_STATE.forId(k.getAlias()),
            ExtendedAttribute.PERSISTENCE_STATE.encode(PersistenceState.PERSISTED));
      }
      statuses[ctr] = entry.getValue().withXAttr(xattr);
      ctr++;
    }
    return statuses;
  }

  private UnionUnderFileSystemUtils() {} // prevent instantiation
}
