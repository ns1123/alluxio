/*************************************************************************
* Copyright (c) 2016 Alluxio, Inc.  All rights reserved.
*
* This software and all information contained herein is confidential and
* proprietary to Alluxio, and is protected by copyright and other
* applicable laws in the United States and other jurisdictions.  You may
* not use, modify, reproduce, distribute, or disclose this software
* without the express written permission of Alluxio.
**************************************************************************/

package alluxio.jobmanager.wire;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The status of a task.
 */
@ThreadSafe
public enum Status {
  CREATED, CANCELED, FAILED, RUNNING, COMPLETED
}
