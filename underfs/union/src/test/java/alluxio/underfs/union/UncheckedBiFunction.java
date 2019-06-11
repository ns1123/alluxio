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
 * A BiFunction which may throw an IOException.
 *
 * @param <T> input 1
 * @param <U> input 2
 * @param <R> output
 * @see {@link java.util.function.BiFunction}
 */
@FunctionalInterface
public interface UncheckedBiFunction<T, U, R> {

  /**
   * The lambda function.
   *
   * @param t input 1
   * @param u input 2
   * @return output
   * @throws IOException depending on lambda parameters
   */
  R apply(T t, U u) throws IOException;
}
