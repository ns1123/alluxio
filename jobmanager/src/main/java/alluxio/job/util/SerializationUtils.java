/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Serialization related utility methods.
 */
@ThreadSafe
public final class SerializationUtils {
  private SerializationUtils() {} // prevent instantiation

  /**
   * Serializes an object into a byte array. When the object is null, returns null.
   *
   * @param obj the object to serialize
   * @return the serialized bytes
   * @throws IOException if the serialization fails
   */
  public static byte[] serialize(Object obj) throws IOException {
    if (obj == null) {
      return null;
    }
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(b)) {
        o.writeObject(obj);
      }
      return b.toByteArray();
    }
  }

  /**
   * Deserializes a byte array into an object. When the bytes are null, returns null.
   *
   * @param bytes the byte array to deserialzie
   * @return the deserialized object
   * @throws IOException if the deserialization fails
   * @throws ClassNotFoundException if no class found to deserialize into
   */
  public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    if (bytes == null) {
      return null;
    }
    try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream o = new ObjectInputStream(b)) {
        return o.readObject();
      }
    }
  }
}
