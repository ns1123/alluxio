/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.underfs.union;

import alluxio.underfs.options.OpenOptions;
import alluxio.util.LogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Virtual input stream that can be used to read a file from a UnionUFS
 *
 * This class initializes the underlying stream lazily. It waits until the first call to read
 * or skip bytes is made.
 */
@NotThreadSafe
public class UnionUnderFileInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(UnionUnderFileInputStream.class);

  /** The options for opening an underlying stream. */
  private final OpenOptions mOptions;
  /** An input stream for an underlying UFS. */
  private InputStream mStream;
  /** The first set of UFSes to attempt reading from. */
  private final Collection<UfsKey> mHintedUfs;
  /** The set of UFSes to fallback on reading if the {@link #mHintedUfs} don't work. */
  private final Collection<UfsKey> mFallbackUfs;
  /** The path to open in the underlying UFS. */
  private final String mPath;

  /**
   * Creates of new instance of {@link UnionUnderFileInputStream}.
   *
   * @param path the path that is being opened
   * @param hintedUfs the UFSes to try opening first
   * @param fallbackUfs the UFSes to fallback on if the hint doesn't work
   * @param options open options
   */
  UnionUnderFileInputStream(String path, Collection<UfsKey> hintedUfs,
      Collection<UfsKey> fallbackUfs, OpenOptions options) {
    mPath = path;
    mOptions = options;
    mStream = null;
    mHintedUfs = hintedUfs;
    mFallbackUfs = fallbackUfs;
  }

  @Override
  public void close() throws IOException {
    if (mStream != null) {
      mStream.close();
    }
    mStream = null;
  }

  @Override
  public int read() throws IOException {
    // If a valid stream exists, read from it.
    initializeStream();
    try {
      return mStream.read();
    } catch (IOException e) {
      cleanup();
      throw e;
    }
  }

  @Override
  public int read(final byte[] b, final int off, final int len) throws IOException {
    initializeStream();
    try {
      return mStream.read(b, off, len);
    } catch (IOException e) {
      cleanup();
      throw e;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    initializeStream();
    try {
      return mStream.skip(n);
    } catch (IOException e) {
      cleanup();
      throw e;
    }
  }

  /**
   * Lazily initializes an input stream from the underlying UFSes.
   *
   * @throws IOException if the stream cannot be initialized
   */
  private synchronized void initializeStream() throws IOException {
    if (mStream != null) {
      return;
    }

    AtomicReference<InputStream> ref = new AtomicReference<>(null);
    // First try based on the hint
    // If error, an IOException is thrown, ref won't be set
    UnionUnderFileSystemUtils.invokeSequentially((ufsKey) -> {
      ref.set(ufsKey.getUfs().open(mPath, mOptions));
    }, () -> ref.get() != null, mHintedUfs);
    // if using the hint doesn't succeed, then try the rest

    if (ref.get() != null) {
      mStream = ref.get();
      return;
    }

    UnionUnderFileSystemUtils.invokeSequentially((ufsKey) -> {
      ref.set(ufsKey.getUfs().open(mPath, mOptions));
    }, () -> ref.get() != null, mFallbackUfs);

    // Nothing succeeded, throw an exception
    if (ref.get() == null) {
      throw new IOException(String.format("Failed to open stream to file at %s", mPath));
    } else {
      mStream = ref.get();
    }
  }

  private void cleanup() {
    if (mStream == null) {
      return;
    }
    try {
      mStream.close();
    } catch (IOException e) {
      LogUtils.warnWithException(LOG, "Failed to close stream at {}", mPath, e);
    }
    mStream = null;
  }
}
