/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.jobmanager.master;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class generates unique job ids.
 *
 *TODO(yupeng) add journal support
 */
@ThreadSafe
public final class JobIdGenerator {
  private final AtomicLong mNextJobId;

  /**
   * Creates a new instance.
   */
  public JobIdGenerator() {
    mNextJobId = new AtomicLong(0);
  }

  public synchronized long getNewJobId() {
    return mNextJobId.getAndIncrement();
  }
}
