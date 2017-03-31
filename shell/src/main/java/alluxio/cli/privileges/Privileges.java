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

package alluxio.cli.privileges;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

/**
 * Tool for performing privilege-related operations.
 */
public final class Privileges {
  private static final Logger LOG = LoggerFactory.getLogger(Privileges.class);

  private final Map<String, Callable<String>> mCommands;

  /**
   * Tool for interacting with Alluxio privileges.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    System.exit(new Privileges().run(args));
  }

  /**
   * Constructs a new privileges command.
   */
  public Privileges() {
    mCommands = ImmutableMap.of(
        "list", new ListPrivilegesCommand(),
        "grant", new GrantPrivilegesCommand(),
        "revoke", new RevokePrivilegesCommand());
  }

  /**
   * @param args arguments to the privileges command
   * @return the exit code
   */
  public int run(String[] args) {
    JCommander jc = new JCommander(this);
    jc.setProgramName("privileges");
    for (Entry<String, Callable<String>> entry : mCommands.entrySet()) {
      jc.addCommand(entry.getKey(), entry.getValue());
    }
    try {
      jc.parse(args);
    } catch (Exception e) {
      System.out.println(e.toString());
      System.out.println();
      jc.usage();
      return -1;
    }
    if (jc.getParsedCommand() == null) {
      jc.usage();
      return -1;
    }
    String result = null;
    try {
      Callable<String> command = mCommands.get(jc.getParsedCommand());
      if (command == null) {
        jc.usage();
        throw new IllegalArgumentException("Unrecognized command: " + jc.getParsedCommand());
      } else {
        result = command.call();
      }
    } catch (Exception e) {
      LOG.error("Privilege command failed. Args: {}", args, e);
      System.out.println("Command failed: " + e.toString());
      return -1;
    }
    if (result != null) {
      System.out.println(result);
    }
    return 0;
  }
}
