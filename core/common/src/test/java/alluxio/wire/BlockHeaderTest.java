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

public class BlockHeaderTest {

  /**
   * Test to convert between a BlockHeader type and a json type.
   *
   * @throws Exception if an error occurs during convert between BlockHeader type and json type
   */
  @Test
  public void json() throws Exception {
    BlockHeader blockHeader = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    BlockHeader other = mapper.readValue(mapper.writeValueAsBytes(blockHeader), BlockHeader.class);
    checkEquality(blockHeader, other);
  }

  /**
   * Test to convert between a thrift type and a wire type.
   */
  @Test
  public void thrift() {
    BlockHeader blockHeader = createRandom();
    BlockHeader other = new BlockHeader(blockHeader.toThrift());
    checkEquality(blockHeader, other);
  }

  /**
   * Check if the two BlockHeader object are equal.
   *
   * @param a the first BlockHeader object to be checked
   * @param b the second BlockHeader object to be checked
   */
  public void checkEquality(BlockHeader a, BlockHeader b) {
    Assert.assertEquals(a.getBlockHeaderSize(), b.getBlockHeaderSize());
    Assert.assertEquals(a.getBlockFooterSize(), b.getBlockFooterSize());
    Assert.assertEquals(a.getChunkHeaderSize(), b.getChunkHeaderSize());
    Assert.assertEquals(a.getChunkSize(), b.getChunkSize());
    Assert.assertEquals(a.getChunkFooterSize(), b.getChunkFooterSize());
    Assert.assertEquals(a.getEncryptionId(), b.getEncryptionId());
    Assert.assertEquals(a, b);
  }

  /**
   * Randomly create a BlockHeader object.
   *
   * @return the created BlockHeader object
   */
  public static BlockHeader createRandom() {
    BlockHeader result = new BlockHeader();
    Random random = new Random();

    int blockHeaderSize = random.nextInt();
    int blockFooterSize = random.nextInt();
    int chunkHeaderSize = random.nextInt();
    int chunkSize = random.nextInt();
    int chunkFooterSize = random.nextInt();
    long encryptionId = random.nextLong();

    result.setBlockHeaderSize(blockHeaderSize);
    result.setBlockFooterSize(blockFooterSize);
    result.setChunkHeaderSize(chunkHeaderSize);
    result.setChunkSize(chunkSize);
    result.setChunkFooterSize(chunkFooterSize);
    result.setEncryptionId(encryptionId);

    return result;
  }
}
