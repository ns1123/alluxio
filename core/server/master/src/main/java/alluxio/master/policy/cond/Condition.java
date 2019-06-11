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

package alluxio.master.policy.cond;

import alluxio.master.policy.meta.InodeState;
import alluxio.master.policy.meta.interval.Interval;
import alluxio.master.policy.meta.interval.IntervalSet;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * A condition represents an expression which evaluates to true or false with respect to a time
 * interval.
 */
public interface Condition {
  LoadingCache<ClassLoader, List<ConditionFactory>> ALL_FACTORIES =
      CacheBuilder.newBuilder().maximumSize(100)
          .build(new CacheLoader<ClassLoader, List<ConditionFactory>>() {
            public List<ConditionFactory> load(ClassLoader key) throws Exception {
              ServiceLoader<ConditionFactory> serviceLoader =
                  ServiceLoader.load(ConditionFactory.class, key);
              List<ConditionFactory> factories = new ArrayList<>();
              for (ConditionFactory factory : serviceLoader) {
                factories.add(factory);
              }
              return factories;
            }
          });

  /**
   * @param interval the time interval to consider the condition for
   * @param path the Alluxio path to evaluate the condition for
   * @param state the state of the path
   * @return an interval set for which the condition evaluates to true
   */
  IntervalSet evaluate(Interval interval, String path, InodeState state);

  /**
   * @return the string representation of the condition
   */
  String serialize();

  /**
   * Creates a condition by deserializing a serialized string. This is the counterpart of
   * {@link #serialize()}.
   *
   * @param serialized the serialized string
   * @return the deserialized condition
   */
  static Condition deserialize(String serialized) {
    // TODO(gpang): This is temporary. This should be migrated to a real lexer/parser.
    // Arbitrary structure is not supported. This is the spec for the supported format:
    //
    // top-condition: NOT(conjunction) | conjunction
    // conjunction: condition | condition AND conjunction
    // condition: customCondition | NOT(customCondition)
    // customCondition: olderThan(time) | xAttr(key, value)
    serialized = serialized.trim();

    // parse for outer NOT(...)
    if (serialized.startsWith("NOT(")) {
      String remaining = serialized.substring("NOT(".length());
      int parenthesisCount = 1;
      int closingIndex = 0;
      for (int i = 0; i < remaining.length(); i++) {
        if (remaining.charAt(i) == '(') {
          parenthesisCount++;
        } else if (remaining.charAt(i) == ')') {
          parenthesisCount--;
        }
        if (parenthesisCount == 0) {
          closingIndex = i;
          break;
        }
      }
      if (closingIndex == remaining.length() - 1) {
        // the closing index is the end of the string, so the "NOT(" is wrapping the entire
        // expression.
        return new LogicalNot(deserialize(remaining.substring(0, remaining.length() - 1)));
      }
    }

      // parse for ... AND ...
    List<String> parts = Arrays.stream(serialized.split(" AND ")).filter(s -> !s.isEmpty())
        .collect(Collectors.toList());

    if (parts.isEmpty()) {
      throw new IllegalStateException("Condition could not be parsed: " + serialized);
    }

    // parse for other conditions
    List<ConditionFactory> factories;
    try {
      factories = ALL_FACTORIES.get(ConditionFactory.class.getClassLoader());
    } catch (ExecutionException e) {
      throw new UnsupportedOperationException(
          "No custom condition factories available: " + e.getCause());
    }

    List<Condition> customConditions = parts.stream().map(
        part -> {
          // check for NOT(...)
          boolean useNot = false;
          if (part.startsWith("NOT(") && part.endsWith(")")) {
            part = part.substring("NOT(".length(), part.length() - 1);
            useNot = true;
          }

          for (ConditionFactory factory : factories) {
            Condition custom = factory.deserialize(part);
            if (custom != null) {
              if (useNot) {
                return new LogicalNot(custom);
              }
              return custom;
            }
          }
          // no custom condition was possible
          throw new IllegalStateException("Custom condition could not be parsed: " + part);
        }).collect(Collectors.toList());

    return new LogicalAnd(customConditions);
  }
}
