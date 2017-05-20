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

package alluxio.util;

import org.slf4j.Logger;

/**
 * JNI related utility functions.
 */
public final class JNIUtils {
  /**
   * Calls {@link System#load} to load the native library at filename, and logs the begin and
   * end of the loading process.
   * Usage of this method is the same as {@link System#load}.
   *
   * @param logger the {@link Logger} for logging the loading process
   * @param filename the native library file to load
   */
  public static void load(Logger logger, String filename) {
    logger.info("Loading native library " + filename);
    System.load(filename);
    logger.info("The native library was loaded");
  }
}
