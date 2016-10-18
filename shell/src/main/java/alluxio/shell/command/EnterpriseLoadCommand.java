/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, makes it resident in memory.
 */
@ThreadSafe
public final class EnterpriseLoadCommand extends WithWildCardPathCommand {
  private static final String REPLICATION = "replication";

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fs the filesystem of Alluxio
   */
  public EnterpriseLoadCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "load";
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(Option.builder(REPLICATION)
        .required(false)
        .hasArg(true)
        .desc("number of replicas to have for each block of the loaded file")
        .build());
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    int replication = 1;
    if (cl.hasOption(REPLICATION)) {
      replication = Integer.parseInt(cl.getOptionValue(REPLICATION));
    }
    load(path, replication);
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void load(AlluxioURI filePath, int replication) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        load(newPath, replication);
      }
    } else {
      Thread thread = alluxio.job.util.JobRestClientUtils.createProgressThread(System.out);
      thread.start();
      try {
        alluxio.job.util.JobRestClientUtils
            .runAndWaitForJob(new alluxio.job.load.LoadConfig(filePath.getPath(), replication), 3);
      } finally {
        thread.interrupt();
      }
    }
    System.out.println(filePath + " loaded");
  }

  @Override
  public String getUsage() {
    return "load [-replication N] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, makes it resident in memory.";
  }
}
