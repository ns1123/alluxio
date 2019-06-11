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

package alluxio.master.policy.action;

import alluxio.master.policy.meta.InodeState;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is the definition of a policy action.
 */
public interface ActionDefinition {
  Pattern ACTION_RE = Pattern.compile("^(?<name>\\w+)\\((?<body>.*)\\)$");

  LoadingCache<ClassLoader, Map<String, ActionDefinitionFactory>> ALL_FACTORIES =
      CacheBuilder.newBuilder().maximumSize(100)
          .build(new CacheLoader<ClassLoader, Map<String, ActionDefinitionFactory>>() {
            public Map<String, ActionDefinitionFactory> load(ClassLoader key) throws Exception {
              ServiceLoader<ActionDefinitionFactory> serviceLoader =
                  ServiceLoader.load(ActionDefinitionFactory.class, key);
              Map<String, ActionDefinitionFactory> factories = new HashMap<>();
              for (ActionDefinitionFactory factory : serviceLoader) {
                String name = factory.getName().toLowerCase();
                ActionDefinitionFactory oldFactory = factories.get(name);
                if (oldFactory != null) {
                  throw new IllegalStateException(String
                      .format("Duplicate action definition name: %s factories: %s, %s", name,
                          oldFactory, factory));
                }
                factories.put(name, factory);
              }
              return factories;
            }
          });

  /**
   * @param ctx the context
   * @param path the alluxio path
   * @param inodeState the inode state
   * @return the ActionExecution instance to manage the execution of the action
   */
  ActionExecution createExecution(ActionExecutionContext ctx, String path, InodeState inodeState);

  /**
   * @return the string representation of the action definition
   */
  String serialize();

  /**
   * Creates an ActionDefinition by deserializing a serialized string. This is the counterpart of
   * {@link #serialize()}.
   *
   * @param serialized the serialized string
   * @return the deserialized ActionDefinition
   */
  static ActionDefinition deserialize(String serialized) {
    // TODO(gpang): This is temporary. This should be migrated to a real lexer/parser.
    // The action definition structure is: actionName(actionBody)

    serialized = serialized.trim();
    Matcher matcher = ACTION_RE.matcher(serialized);
    if (!matcher.matches()) {
      throw new IllegalStateException(
          "Action definition string cannot be deserialized: " + serialized);
    }

    String name = matcher.group("name");
    String body = matcher.group("body");

    Map<String, ActionDefinitionFactory> factories;
    try {
      factories = ALL_FACTORIES.get(ActionDefinitionFactory.class.getClassLoader());
    } catch (ExecutionException e) {
      throw new IllegalStateException("No action definition factories available: " + e.getCause());
    }

    ActionDefinitionFactory factory = factories.get(name.toLowerCase());
    if (factory == null) {
      throw new IllegalStateException("No action definition factory available for name: " + name);
    }
    return factory.create(body);
  }
}
