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

package alluxio.concurrent;

import alluxio.security.authentication.AuthenticatedClientUser;

import org.apache.thrift.server.TThreadPoolServer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Factory methods to create executors.
 */
public final class Executors {

  /**
   * Creates a default instance of {@link ExecutorServiceWithCallback}.
   *
   * @param args the ThreadPoolServer args
   * @param runnable the callback
   * @return the executor service instance
   */
  public static ExecutorService createDefaultExecutorService(
      TThreadPoolServer.Args args, Runnable runnable) {
    SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<Runnable>();
    return new ExecutorServiceWithCallback(
        new ThreadPoolExecutor(args.minWorkerThreads, args.maxWorkerThreads, args.stopTimeoutVal,
            TimeUnit.SECONDS, executorQueue), runnable);
  }

  /**
   * Creates a default instance of {@link ExecutorServiceWithCallback} with default callback
   * that clears the current client user. This is used when the security is on.
   *
   * @param args the ThreadPoolServer args
   * @return the executor service instance
   */
  public static ExecutorService createDefaultExecutorServiceWithSecurityOn(
      TThreadPoolServer.Args args) {
    return createDefaultExecutorService(args, new Runnable() {
      @Override
      public void run() {
        AuthenticatedClientUser.remove();
      }
    });
  }

  /**
   * Private constructor.
   */
  private Executors() {
  }

}
