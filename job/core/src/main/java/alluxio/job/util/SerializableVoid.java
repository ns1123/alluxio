/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.util;

import java.io.Serializable;

/**
 * Same as {@link java.lang.Void}, but implementing Serializable.
 */
public final class SerializableVoid implements Serializable {
  private static final long serialVersionUID = -758943908309970720L;

  private SerializableVoid() {} // Not intendend for instantiation.
}
