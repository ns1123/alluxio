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

package alluxio.util;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;

import com.google.common.base.Objects;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for working with tiered identity.
 */
@ThreadSafe
public final class TieredIdentityUtils {

  /**
   * Locality comparison for wire type locality tiers, two locality tiers matches if both name and
   * values are equal, or for the "node" tier, if the node names resolve to the same IP address.
   *
   * @param tier a wire type locality tier
   * @param otherTier a wire type locality tier to compare to
   * @param resolveIpAddress whether or not to resolve hostnames to IP addresses for node locality
   * @return true if the wire type locality tier matches the given tier
   */
  public static boolean matches(TieredIdentity.LocalityTier tier,
      TieredIdentity.LocalityTier otherTier, boolean resolveIpAddress) {
    String otherTierName = otherTier.getTierName();
    if (!tier.getTierName().equals(otherTierName)) {
      return false;
    }
    String otherTierValue = otherTier.getValue();
    if (tier.getValue() != null && tier.getValue().equals(otherTierValue)) {
      return true;
    }
    // For node tiers, attempt to resolve hostnames to IP addresses, this avoids common
    // misconfiguration errors where a worker is using one hostname and the client is using
    // another.
    if (resolveIpAddress) {
      if (Constants.LOCALITY_NODE.equals(tier.getTierName())) {
        try {
          String tierIpAddress = NetworkAddressUtils.resolveIpAddress(tier.getValue());
          String otherTierIpAddress = NetworkAddressUtils.resolveIpAddress(otherTierValue);
          if (tierIpAddress != null && tierIpAddress.equals(otherTierIpAddress)) {
            return true;
          }
        } catch (UnknownHostException e) {
          return false;
        }
      }
    }
    return false;
  }

  // ALLUXIO CS REPLACE
  // /**
  // * @param tieredIdentity the tiered identity
  // * @param identities the tiered identities to compare to
  // * @param conf Alluxio configuration
  // * @return the identity closest to this one. If none of the identities match, the first identity
  // *         is returned
  // */
  // ALLUXIO CS WITH
  /**
   *
   * @param tieredIdentity the tiered identity
   * @param identities the tiered identities to compare to
<<<<<<< HEAD
   * @param conf Alluxio configuration
   * @return the identity closest to this one; or Optional.empty if none of the identities match
   *         within a strict tier. If none of the identities match and no strict tiers are defined,
   *         the first identity is returned
||||||| merged common ancestors
   * @param resolveIpAddress whether or not to resolve IP addresses for node locality
   * @return the identity closest to this one. If none of the identities match, the first identity
   *         is returned
=======
   * @param conf Alluxio configuration
   * @return the identity closest to this one. If none of the identities match, the first identity
   *         is returned
>>>>>>> upstream-os/master
   */
  // ALLUXIO CS END
  public static Optional<TieredIdentity> nearest(TieredIdentity tieredIdentity,
      List<TieredIdentity> identities, AlluxioConfiguration conf) {
    if (identities.isEmpty()) {
      return Optional.empty();
    }
    for (TieredIdentity.LocalityTier tier : tieredIdentity.getTiers()) {
      for (TieredIdentity identity : identities) {
        for (TieredIdentity.LocalityTier otherTier : identity.getTiers()) {
<<<<<<< HEAD
          if (tier != null && matches(tier, otherTier,
              conf.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP))) {
||||||| merged common ancestors
          if (tier != null && matches(tier, otherTier, resolveIpAddress)) {
=======
          if (tier != null
              && matches(tier, otherTier, conf.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP))) {
>>>>>>> upstream-os/master
            return Optional.of(identity);
          }
        }
      }
      // ALLUXIO CS ADD
      if (conf.isSet(PropertyKey.Template.LOCALITY_TIER_STRICT.format(tier.getTierName()))
          && conf.getBoolean(PropertyKey.Template.LOCALITY_TIER_STRICT
          .format(tier.getTierName()))) {
        return Optional.empty();
      }
      // ALLUXIO CS END
    }
    return Optional.of(identities.get(0));
  }
  // ALLUXIO CS ADD

  /**
   * @param tiers a set of locality tiers to compare from
   * @param other a tiered identity to compare to
   * @param conf Alluxio configuration
   * @return whether this tiered identity matches the given tiered identity in all strict tiers
   */
  public static boolean strictTiersMatch(List<TieredIdentity.LocalityTier> tiers,
      TieredIdentity other, AlluxioConfiguration conf) {
    for (TieredIdentity.LocalityTier t : tiers) {
      alluxio.conf.PropertyKey strictKey =
          alluxio.conf.PropertyKey.Template.LOCALITY_TIER_STRICT.format(t.getTierName());
      if (conf.isSet(strictKey)
          && conf.getBoolean(strictKey)) {
        for (TieredIdentity.LocalityTier tier : other.getTiers()) {
          if (Objects.equal(tier.getTierName(), t.getTierName())) {
            // unspecified locality != unspecified locality
            if (tier.getValue() == null
                || t.getValue() == null
                || !Objects.equal(tier.getValue(), t.getValue())) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }
  // ALLUXIO CS END

  private TieredIdentityUtils() {} // prevent instantiation
}
