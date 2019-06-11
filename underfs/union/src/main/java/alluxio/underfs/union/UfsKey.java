/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import alluxio.underfs.UnderFileSystem;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * A UfsKey used to identiy specific UFSes within a UnionUnderFileSystem.
 */
public class UfsKey implements Comparable<UfsKey> {
  private final String mAlias;
  private final int mPriority;
  private final UnderFileSystem mUfs;

  UfsKey(String alias, int priority, UnderFileSystem ufs) {
    mAlias = Preconditions.checkNotNull(alias);
    mPriority = priority;
    mUfs = ufs;
  }

  /**
   * @return the alias associated with this key
   */
  public String getAlias() {
    return mAlias;
  }

  /**
   * @return the alias associated with this key
   */
  public int getPriority() {
    return mPriority;
  }

  /**
   * @return the ufs associated with the key
   */
  public UnderFileSystem getUfs() {
    return mUfs;
  }

  @Override
  public int compareTo(UfsKey o) {
    if (mPriority < o.mPriority) {
      return -1;
    } else if (mPriority == o.mPriority) {
      return 0;
    }
    return 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof UfsKey)) {
      return false;
    }

    UfsKey other = (UfsKey) o;

    return Objects.equal(mAlias, other.mAlias)
        && Objects.equal(mPriority, other.mPriority)
        && Objects.equal(mUfs, other.mUfs);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAlias, mPriority, mUfs);
  }
}
