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

package alluxio.worker.security;

import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidCapabilityException;
import alluxio.proto.security.CapabilityProto;
import alluxio.security.authorization.Mode;
import alluxio.security.capability.Capability;
import alluxio.security.capability.CapabilityKey;
import alluxio.util.CommonUtils;

import org.apache.curator.utils.ThreadUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for {@link CapabilityCache}.
 */
public final class CapabilityCacheTest {
  private final long mKeyId = 1L;
  private final long mFileId = 2L;
  private final String mEncodingKey = "mykey";
  private final String mUsername = "testuser";

  private final CapabilityKey mKey = CapabilityKey.defaults()
      .setKeyId(mKeyId)
      .setEncodedKey(mEncodingKey.getBytes())
      .setExpirationTimeMs(CommonUtils.getCurrentMs() + 1000 * 1000);

  private final CapabilityProto.Content mReadContent = CapabilityProto.Content.newBuilder()
      .setUser(mUsername)
      .setFileId(mFileId)
      .setAccessMode(Mode.Bits.READ.ordinal())
      .setExpirationTimeMs(CommonUtils.getCurrentMs() + 1000 * 1000).build();

  private CapabilityCache mCache;

  @Before
  public void before() {
    mCache = new CapabilityCache(CapabilityCache.Options.defaults().setCapabilityKey(mKey));
  }

  @After
  public void after() {
    mCache.close();
  }

  @Test
  public void addCapabilityValid() throws Exception {
    alluxio.thrift.Capability capabilityThrift =  new Capability(mKey, mReadContent).toThrift();
    mCache.incrementUserConnectionCount(mUsername);
    mCache.addCapability(capabilityThrift);
    mCache.checkAccess(mUsername, mFileId, Mode.Bits.READ);

    try {
      mCache.checkAccess(mUsername, mFileId, Mode.Bits.WRITE);
      Assert.fail();
    } catch (AccessControlException e) {
      // expected
    }
    mCache.decrementUserConnectionCount(mUsername);
  }

  @Test
  public void addCapabilityInvalid() throws Exception {
    mCache.incrementUserConnectionCount(mUsername);

    // add (null) is an no-op
    mCache.addCapability(null);

    alluxio.thrift.Capability capabilityThrift =  new Capability(mKey, mReadContent).toThrift();
    // Invalidate the content
    capabilityThrift.getContent()[0]++;

    try {
      mCache.addCapability(capabilityThrift);
      Assert.fail();
    } catch (InvalidCapabilityException e) {
      // expected
    }

    mCache.decrementUserConnectionCount(mUsername);
  }

  @Test
  public void gc() throws Exception {
    CapabilityProto.Content content = CapabilityProto.Content.newBuilder()
        .setUser(mUsername)
        .setFileId(mFileId)
        .setAccessMode(Mode.Bits.READ.ordinal())
        .setExpirationTimeMs(CommonUtils.getCurrentMs() + 100).build();
    try (CapabilityCache cache = new CapabilityCache(
        CapabilityCache.Options.defaults().setCapabilityKey(mKey).setGCIntervalMs(1))) {
      alluxio.thrift.Capability capabilityThrift = new Capability(mKey, content).toThrift();
      cache.incrementUserConnectionCount(mUsername);
      cache.addCapability(capabilityThrift);
      CommonUtils.sleepMs(1000);

      try {
        cache.checkAccess(mUsername, mFileId, Mode.Bits.READ);
        Assert.fail();
      } catch (InvalidCapabilityException e) {
        // expected
      }
    }
  }

  @Test
  public void highConcurrencyUserConnectionCount() throws Exception {
    final ArrayList<String> users = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String user = "user" + i;
      users.add(user);
    }

    final AtomicBoolean fail = new AtomicBoolean(false);

    class IncDecUser implements Runnable {
      @Override
      public void run() {
        for (int round = 0; round < 1000; round++) {
          try {
            // Get a random user.
            String user = users.get((int) (Math.random() * users.size()));

            mCache.incrementUserConnectionCount(user);
            CommonUtils.sleepMs(2);
            mCache.decrementUserConnectionCount(user);
          } catch (Throwable e) {
            fail.set(true);
          }
        }
      }
    }

    ExecutorService executor = ThreadUtils.newFixedThreadPool(10, "");
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(new IncDecUser()));
    }

    for (Future f : futures) {
      f.get();
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    Assert.assertFalse(fail.get());
  }

  @Test
  public void highConcurrencyAddCapability() throws Exception {
    final ConcurrentLinkedQueue<String> users = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < 10; i++) {
      String user = "user" + i;
      users.add(user);
      mCache.incrementUserConnectionCount(user);
    }

    final ConcurrentLinkedQueue<String> usersCopy = new ConcurrentLinkedQueue<>();
    usersCopy.addAll(users);

    final AtomicBoolean fail = new AtomicBoolean(false);

    class AddUser implements Runnable {
      @Override
      public void run() {
        String user = users.poll();
        if (user != null) {
          for (int i = 0; i < 10; i++) {
            for (alluxio.thrift.Capability c : getCapabilityForUser(user)) {
              try {
                mCache.addCapability(c);
              } catch (InvalidCapabilityException e) {
                fail.set(true);
              }
            }
            CommonUtils.sleepMs(10);
          }
        }
      }
    }

    class CheckUser implements Runnable {
      @Override
      public void run() {
        String user = usersCopy.poll();
        for (int i = 0; i < 10; i++) {
          CommonUtils.sleepMs(10);
          try {
            mCache.checkAccess(user, mFileId, Mode.Bits.READ);
            mCache.checkAccess(user, mFileId + 1, Mode.Bits.WRITE);
          } catch (InvalidCapabilityException e) {
            // This can happen if the capability is not yet added. ignore.
          } catch (AccessControlException e) {
            fail.set(true);
          }

          try {
            mCache.checkAccess(user, mFileId, Mode.Bits.WRITE);
            fail.set(true);
          } catch (InvalidCapabilityException | AccessControlException e) {
            // expected.
          }
          try {
            mCache.checkAccess(user, mFileId + 1, Mode.Bits.READ);
            fail.set(true);
          } catch (InvalidCapabilityException | AccessControlException e) {
            // expected.
          }
        }
      }
    }

    ExecutorService addUserExecutor = ThreadUtils.newFixedThreadPool(10, "");
    ExecutorService checkUserExecutor = ThreadUtils.newFixedThreadPool(10, "");
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(addUserExecutor.submit(new AddUser()));
      futures.add(checkUserExecutor.submit(new CheckUser()));
    }

    for (Future f : futures) {
      f.get();
    }

    addUserExecutor.shutdown();
    checkUserExecutor.shutdown();
    addUserExecutor.awaitTermination(10, TimeUnit.MINUTES);
    checkUserExecutor.awaitTermination(10, TimeUnit.MINUTES);

    Assert.assertFalse(fail.get());
  }

  private List<alluxio.thrift.Capability> getCapabilityForUser(String user) {
    CapabilityProto.Content read = CapabilityProto.Content.newBuilder()
        .setUser(user)
        .setFileId(mFileId)
        .setAccessMode(Mode.Bits.READ.ordinal())
        .setExpirationTimeMs(CommonUtils.getCurrentMs() + 1000 * 1000).build();

    CapabilityProto.Content write = CapabilityProto.Content.newBuilder()
        .setUser(user)
        .setFileId(mFileId + 1)
        .setAccessMode(Mode.Bits.WRITE.ordinal())
        .setExpirationTimeMs(CommonUtils.getCurrentMs() + 1000 * 1000).build();
    ArrayList<alluxio.thrift.Capability> contents = new ArrayList<>();

    contents.add(new Capability(mKey, read).toThrift());
    contents.add(new Capability(mKey, write).toThrift());
    return contents;
  }
}
