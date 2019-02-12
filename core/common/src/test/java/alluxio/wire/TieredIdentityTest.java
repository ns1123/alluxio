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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.TieredIdentityFactory;
import alluxio.util.CommonUtils;
import alluxio.grpc.GrpcUtils;
import alluxio.wire.TieredIdentity.LocalityTier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Unit tests for {@link TieredIdentity}.
 */
public class TieredIdentityTest {

  private InstancedConfiguration mConfiguration = ConfigurationTestUtils.defaults();

  @Before
  public void before() {
    mConfiguration = ConfigurationTestUtils.defaults();
  }

  @Test
  public void nearest() throws Exception {
    TieredIdentity id1 = TieredIdentityFactory.fromString("node=A,rack=rack1", mConfiguration);
    TieredIdentity id2 = TieredIdentityFactory.fromString("node=B,rack=rack2", mConfiguration);
    TieredIdentity id3 = TieredIdentityFactory.fromString("node=C,rack=rack2", mConfiguration);
    List<TieredIdentity> identities = Arrays.asList(id1, id2, id3);

<<<<<<< HEAD
    assertSame(id1,
        TieredIdentityFactory.fromString("node=D,rack=rack1").nearest(identities).get());
    assertSame(id2,
        TieredIdentityFactory.fromString("node=B,rack=rack2").nearest(identities).get());
    assertSame(id3,
        TieredIdentityFactory.fromString("node=C,rack=rack2").nearest(identities).get());
    assertSame(id1,
        TieredIdentityFactory.fromString("node=D,rack=rack3").nearest(identities).get());
    // ALLUXIO CS ADD
    try (java.io.Closeable c = new alluxio.ConfigurationRule(
        alluxio.PropertyKey.Template.LOCALITY_TIER_STRICT.format(alluxio.Constants.LOCALITY_RACK), "true")
            .toResource()) {
      org.junit.Assert.assertFalse(
          TieredIdentityFactory.fromString("node=D,rack=rack3").nearest(identities).isPresent());
    }
    // ALLUXIO CS END
=======
    boolean resolveIp = mConfiguration.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP);
    assertSame(id1, TieredIdentityUtils
        .nearest(TieredIdentityFactory.fromString("node=D,rack=rack1", mConfiguration), identities,
            resolveIp).get());
    assertSame(id2, TieredIdentityUtils
        .nearest(TieredIdentityFactory.fromString("node=B,rack=rack2", mConfiguration), identities,
            resolveIp).get());
    assertSame(id3, TieredIdentityUtils
        .nearest(TieredIdentityFactory.fromString("node=C,rack=rack2", mConfiguration), identities,
            resolveIp).get());
    assertSame(id1, TieredIdentityUtils
        .nearest(TieredIdentityFactory.fromString("node=D,rack=rack3", mConfiguration), identities,
            resolveIp).get());
>>>>>>> c1daabcbd9a604557d7ca3d05d3d8a63f95d2885
  }

  @Test
  public void json() throws Exception {
    TieredIdentity tieredIdentity = createRandomTieredIdentity();
    ObjectMapper mapper = new ObjectMapper();
    TieredIdentity other =
        mapper.readValue(mapper.writeValueAsBytes(tieredIdentity), TieredIdentity.class);
    checkEquality(tieredIdentity, other);
  }

  @Test
  public void proto() {
    TieredIdentity tieredIdentity = createRandomTieredIdentity();
    TieredIdentity other = GrpcUtils.fromProto(GrpcUtils.toProto(tieredIdentity));
    checkEquality(tieredIdentity, other);
  }

  @Test
  public void matchByStringEquality() {
    LocalityTier lt1 = new LocalityTier("node", "NonResolvableHostname-A");
    LocalityTier lt2 = new LocalityTier("node", "NonResolvableHostname-A");
    LocalityTier lt3 = new LocalityTier("node", "NonResolvableHostname-B");
    LocalityTier lt4 = new LocalityTier("rack", "NonResolvableHostname-A");
    LocalityTier lt5 = new LocalityTier("rack", "NonResolvableHostname-B");
    LocalityTier lt6 = new LocalityTier("rack", "NonResolvableHostname-B");
    LocalityTier lt7 = new LocalityTier("rack", "");
    LocalityTier lt8 = new LocalityTier("node", "NonResolvableHostname-A");
    LocalityTier lt9 = new LocalityTier("node", "");
<<<<<<< HEAD
    assertTrue(lt1.matches(lt1));
    assertTrue(lt1.matches(lt2));
    assertFalse(lt2.matches(lt3));
    assertTrue(lt5.matches(lt6));
    assertFalse(lt4.matches(lt5));
    assertFalse(lt6.matches(lt7));
    assertFalse(lt8.matches(lt9));
=======
    assertTrue(TieredIdentityUtils.matches(lt1, lt1, true));
    assertTrue(TieredIdentityUtils.matches(lt1, lt2, true));
    assertFalse(TieredIdentityUtils.matches(lt2, lt3, true));
    assertTrue(TieredIdentityUtils.matches(lt5, lt6, true));
    assertFalse(TieredIdentityUtils.matches(lt4, lt5, true));
    assertFalse(TieredIdentityUtils.matches(lt6, lt7, true));
    assertFalse(TieredIdentityUtils.matches(lt8, lt9, true));
>>>>>>> c1daabcbd9a604557d7ca3d05d3d8a63f95d2885
  }

  @Test
  public void matchByIpResolution() throws Exception {
    assumeTrue(InetAddress.getByName("localhost").getHostAddress().equals("127.0.0.1"));
    LocalityTier lt1 = new LocalityTier("node", "localhost");
    LocalityTier lt2 = new LocalityTier("node", "127.0.0.1");

<<<<<<< HEAD
    Configuration.set(PropertyKey.LOCALITY_COMPARE_NODE_IP, "true");
    assertTrue(lt1.matches(lt2));

    Configuration.set(PropertyKey.LOCALITY_COMPARE_NODE_IP, "false");
    assertFalse(lt1.matches(lt2));
=======
    assertTrue(TieredIdentityUtils.matches(lt1, lt2, true));
    assertFalse(TieredIdentityUtils.matches(lt1, lt2, false));
>>>>>>> c1daabcbd9a604557d7ca3d05d3d8a63f95d2885
  }

  public void string() {
    TieredIdentity identity = new TieredIdentity(
        Arrays.asList(new LocalityTier("k1", "v1"), new LocalityTier("k2", "v2")));
    assertEquals("TieredIdentity(k1=v1, k2=v2)", identity.toString());
  }

  public void checkEquality(TieredIdentity a, TieredIdentity b) {
    assertEquals(a.getTiers(), b.getTiers());
    assertEquals(a, b);
  }

  public static TieredIdentity createRandomTieredIdentity() {
    return new TieredIdentity(
        Arrays.asList(createRandomLocalityTier(), createRandomLocalityTier()));
  }

  private static LocalityTier createRandomLocalityTier() {
    Random random = new Random();

    String tier = CommonUtils.randomAlphaNumString(random.nextInt(10) + 1);
    String value = CommonUtils.randomAlphaNumString(random.nextInt(10) + 1);
    return new LocalityTier(tier, value);
  }
}
