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
 * A functional interface for which the lambda passed may throw an IOException.
 *
 * @param <T> input type
 * @param <R> return type
 */
@FunctionalInterface
public interface UncheckedIOFunction<T, R> {

  /**
   * A function which may throw an IOException.
   *
   * @param t function input
   * @return function output
   * @throws IOException depending on the lambda parameters
   */
  R apply(T t) throws IOException;
}
