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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Renames a file or directory specified by args. Will fail if the new path name already exists.
 */
@ThreadSafe
public final class MvCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public MvCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "mv";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);
<<<<<<< HEAD

    // ALLUXIO CS REPLACE
    // mFileSystem.rename(srcPath, dstPath);
    // ALLUXIO CS WITH
    if (mFileSystem.exists(dstPath)) {
      throw new RuntimeException(dstPath + " already exists");
    }
    try {
      mFileSystem.rename(srcPath, dstPath);
    } catch (Exception e) {
      // Try the job service in case it's a cross-mount move. In the future we should improve the
      // FileSystem API to make it easier to tell whether a move is cross-mount.
      Thread thread = alluxio.job.util.JobRestClientUtils
          .createProgressThread(2 * alluxio.Constants.SECOND_MS, System.out);
      thread.start();
      try {
        alluxio.job.util.JobRestClientUtils.runAndWaitForJob(
            new alluxio.job.move.MoveConfig(srcPath.getPath(), dstPath.getPath(), null, true), 3);
      } finally {
        thread.interrupt();
      }
    }
    // ALLUXIO CS END
||||||| merged common ancestors
    // ALLUXIO CS REPLACE
    // mFileSystem.rename(srcPath, dstPath);
    // ALLUXIO CS WITH
    if (mFileSystem.exists(dstPath)) {
      throw new RuntimeException(dstPath + " already exists");
    }
    try {
      mFileSystem.rename(srcPath, dstPath);
    } catch (Exception e) {
      // Try the job service in case it's a cross-mount move. In the future we should improve the
      // FileSystem API to make it easier to tell whether a move is cross-mount.
      Thread thread = JobRestClientUtils.createProgressThread(2 * Constants.SECOND_MS, System.out);
      thread.start();
      try {
        JobRestClientUtils
            .runAndWaitForJob(new MoveConfig(srcPath.getPath(), dstPath.getPath(), null, true), 3);
      } finally {
        thread.interrupt();
      }
    }
    // ALLUXIO CS END
=======
    mFileSystem.rename(srcPath, dstPath);
>>>>>>> upstream/enterprise-1.3
    System.out.println("Renamed " + srcPath + " to " + dstPath);
  }

  @Override
  public String getUsage() {
    return "mv <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Renames a file or directory.";
  }
}
