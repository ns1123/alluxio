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

package alluxio.wire;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class FileMetadataTest {

  /**
   * Test to convert between a FileMetadata type and a json type.
   *
   * @throws Exception if an error occurs during convert between FileMetadata type and json type
   */
  @Test
  public void json() throws Exception {
    FileMetadata fileMetadata = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    FileMetadata other = mapper.readValue(mapper.writeValueAsBytes(fileMetadata), FileMetadata.class);
    checkEquality(fileMetadata, other);
  }

  /**
   * Test to convert between a thrift type and a wire type.
   */
  @Test
  public void thrift() {
    FileMetadata fileMetadata = createRandom();
    FileMetadata other = new FileMetadata(fileMetadata.toThrift());
    checkEquality(fileMetadata, other);
  }

  /**
   * Check if the two FileMetadata object are equal.
   *
   * @param a the first FileMetadata object to be checked
   * @param b the second FileMetadata object to be checked
   */
  public void checkEquality(FileMetadata a, FileMetadata b) {
    Assert.assertEquals(a.getBlockHeaderSize(), b.getBlockHeaderSize());
    Assert.assertEquals(a.getBlockFooterSize(), b.getBlockFooterSize());
    Assert.assertEquals(a.getChunkHeaderSize(), b.getChunkHeaderSize());
    Assert.assertEquals(a.getChunkSize(), b.getChunkSize());
    Assert.assertEquals(a.getChunkFooterSize(), b.getChunkFooterSize());
    Assert.assertEquals(a.getPhysicalBlockSize(), b.getPhysicalBlockSize());
    Assert.assertEquals(a.getEncryptionId(), b.getEncryptionId());
    Assert.assertEquals(a, b);
  }

  /**
   * Randomly create a FileMetadata object.
   *
   * @return the created FileMetadata object
   */
  public static FileMetadata createRandom() {
    FileMetadata result = new FileMetadata();
    Random random = new Random();

    int blockHeaderSize = random.nextInt();
    int blockFooterSize = random.nextInt();
    int chunkHeaderSize = random.nextInt();
    int chunkSize = random.nextInt();
    int chunkFooterSize = random.nextInt();
    int physicalBlockSize = random.nextInt();
    long encryptionId = random.nextLong();

    result.setBlockHeaderSize(blockHeaderSize);
    result.setBlockFooterSize(blockFooterSize);
    result.setChunkHeaderSize(chunkHeaderSize);
    result.setChunkSize(chunkSize);
    result.setChunkFooterSize(chunkFooterSize);
    result.setPhysicalBlockSize(physicalBlockSize);
    result.setEncryptionId(encryptionId);

    return result;
  }
}
