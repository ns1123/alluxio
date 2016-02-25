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

package alluxio.jobmanager.util;

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
  private SerializationUtils() {}

  /**
   * Serializes an object into a byte array.
   *
   * @param obj the object to serialize
   * @return the serialized bytes
   * @throws IOException if the serialization fails
   */
  public static byte[] serialize(Object obj) throws IOException {
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(b)) {
        o.writeObject(obj);
      }
      return b.toByteArray();
    }
  }

  /**
   * Deserializes a byte array into an object.
   *
   * @param bytes the byte array to deserialzie
   * @return the deserialized object
   * @throws IOException if the deserialization fails
   * @throws ClassNotFoundException if no class found to deserialize into
   */
  public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream o = new ObjectInputStream(b)) {
        return o.readObject();
      }
    }
  }
}
