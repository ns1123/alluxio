/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.cli.privileges;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Tool for performing privilege-related operations.
 */
public final class Privileges {
  private static final Logger LOG = LoggerFactory.getLogger(Privileges.class);

  private static final Map<String, Callable<String>> SUBCOMMANDS = ImmutableMap.of(
      "list", new ListPrivilegesCommand(),
      "grant", new GrantPrivilegesCommand(),
      "revoke", new RevokePrivilegesCommand());

  /**
   * Tool for interacting with Alluxio privileges.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    System.exit(privileges(args));
  }

  /**
   * @param args arguments to the privileges command
   * @return the exit code
   */
  private static int privileges(String[] args) {
    Privileges p = new Privileges();
    JCommander jc = new JCommander(p);
    jc.setProgramName("privileges");
    for (String subcommand : SUBCOMMANDS.keySet()) {
      jc.addCommand(subcommand, SUBCOMMANDS.get(subcommand));
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
      Callable<String> command = SUBCOMMANDS.get(jc.getParsedCommand());
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

  private Privileges() {} // Not intended for instantiation.
}
