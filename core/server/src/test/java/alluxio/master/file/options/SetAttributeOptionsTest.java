/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.options;

<<<<<<< HEAD
||||||| merged common ancestors
import alluxio.Configuration;

=======
import alluxio.CommonTestUtils;

>>>>>>> OPENSOURCE/master
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Unit tests for {@link SetAttributeOptions}.
 */
public class SetAttributeOptionsTest {
  /**
<<<<<<< HEAD
   * Tests the {@link SetAttributeOptions#defaults()} method.
   */
  @Test
  public void defaultsTest() {
    SetAttributeOptions options = SetAttributeOptions.defaults();

    Assert.assertNull(options.getPinned());
    Assert.assertNull(options.getTtl());
    Assert.assertNull(options.getPersisted());
  }

  /**
   * Tests getting and setting fields.
||||||| merged common ancestors
   * Tests the {@link SetAttributeOptions.Builder}.
=======
   * Tests the {@link SetAttributeOptions#defaults()} method.
>>>>>>> OPENSOURCE/master
   */
  @Test
<<<<<<< HEAD
  public void fieldsTest() {
||||||| merged common ancestors
  public void builderTest() {
=======
  public void defaultsTest() {
    SetAttributeOptions options = SetAttributeOptions.defaults();

    Assert.assertNull(options.getPinned());
    Assert.assertNull(options.getTtl());
    Assert.assertNull(options.getPersisted());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fieldsTest() {
>>>>>>> OPENSOURCE/master
    Random random = new Random();
    Boolean pinned = random.nextBoolean();
    Long ttl = random.nextLong();
    Boolean persisted = random.nextBoolean();

    SetAttributeOptions options = SetAttributeOptions.defaults()
        .setPinned(pinned)
        .setTtl(ttl)
        .setPersisted(persisted);

    Assert.assertEquals(pinned, options.getPinned());
    Assert.assertEquals(ttl, options.getTtl());
    Assert.assertEquals(persisted, options.getPersisted());
  }
<<<<<<< HEAD
||||||| merged common ancestors

  /**
   * Tests the {@link CreateDirectoryOptions#defaults()} method.
   */
  @Test
  public void defaultsTest() {
    SetAttributeOptions options = SetAttributeOptions.defaults();

    Assert.assertNull(options.getPinned());
    Assert.assertNull(options.getTtl());
    Assert.assertNull(options.getPersisted());
  }
=======

  @Test
  public void testEquals() throws Exception {
    CommonTestUtils.testEquals(SetAttributeOptions.class, "mOperationTimeMs");
  }
>>>>>>> OPENSOURCE/master
}
