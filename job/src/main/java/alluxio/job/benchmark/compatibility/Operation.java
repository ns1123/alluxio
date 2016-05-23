/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.benchmark.compatibility;

/**
 * An operation which can be generated and validated, for compatibility tests.
 */
public interface Operation {

  /**
   * Generates operations.
   *
   * @throws Exception if an error occurs
   */
  void generate() throws Exception;

  /**
   * Validates the operations performed in {@link #generate()}.
   *
   * @throws Exception if validation fails
   */
  void validate() throws Exception;
}
