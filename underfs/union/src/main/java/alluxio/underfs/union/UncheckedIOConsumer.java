/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import java.io.IOException;

/**
 * A functional interface which may accept a function which throws an IOException without a
 * try-catch block.
 *
 * @param <T> consuming type
 */
@FunctionalInterface
public interface UncheckedIOConsumer<T> {

  /**
   * A consumer function which may throw an IOException.
   *
   * @param t the consumer input
   * @throws IOException depending on lambda parameters
   */
  void apply(T t) throws IOException;
}
