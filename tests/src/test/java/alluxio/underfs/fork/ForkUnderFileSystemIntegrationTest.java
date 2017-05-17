package alluxio.underfs.fork;

import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Integration tests for {@link ForkUnderFileSystem}.
 */
public class ForkUnderFileSystemIntegrationTest {
  @Rule
  public TemporaryFolder mUfsFolderA = new TemporaryFolder();
  @Rule
  public TemporaryFolder mUfsFolderB = new TemporaryFolder();

  private Random random = new Random();
  private String mUfsPathA;
  private String mUfsPathB;
  private UnderFileSystem mUnderFileSystem;

  @Before
  public void before() {
    mUfsPathA = mUfsFolderA.getRoot().getAbsolutePath();
    mUfsPathB = mUfsFolderB.getRoot().getAbsolutePath();
    Map<String, String> properties = new HashMap<>();
    properties.put("alluxio-fork.A.ufs", mUfsPathA);
    properties.put("alluxio-fork.B.ufs", mUfsPathB);
    mUnderFileSystem = UnderFileSystem.Factory.create("alluxio-fork://", properties);
  }

  @Test
  public void directoryLifeCycle() throws Exception {
    String[] filenames = { "test-dir", "alluxio-fork:///test-dir"};
    for (String filename : filenames) {
      // Initially the file should not exist in either UFS.
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertFalse(mUnderFileSystem.exists(filename));
      // Create it and check it exists in both UFSes.
      mUnderFileSystem.mkdirs(filename);
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertTrue(mUnderFileSystem.exists(filename));
      System.out.println(mUnderFileSystem.getDirectoryStatus(filename));
      // Delete it and check it does not exist in either UFS.
      mUnderFileSystem.deleteDirectory(filename);
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertFalse(mUnderFileSystem.exists(filename));
    }
  }

  @Test
  public void fileLifeCycle() throws Exception {
    String[] filenames = { "test-file", "alluxio-fork:///test-file"};
    for (String filename : filenames) {
      // Initially the directory should not exist in either UFS.
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertFalse(mUnderFileSystem.exists(filename));
      // Create it and check it exists in both UFSes.
      mUnderFileSystem.create(filename).close();
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertTrue(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertTrue(mUnderFileSystem.exists(filename));
      System.out.println(mUnderFileSystem.getFileStatus(filename));
      // Delete it and check it exists in both UFSes.
      mUnderFileSystem.deleteFile(filename);
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathA, normalize(filename))));
      Assert.assertFalse(exists(PathUtils.concatPath(mUfsPathB, normalize(filename))));
      Assert.assertFalse(mUnderFileSystem.exists(filename));
    }
  }

  @Test
  public void directoryStatus() throws Exception {
    String[] filenames = { "test-dir", "alluxio-fork:///test-dir"};
    for (String filename : filenames) {
      mUnderFileSystem.mkdirs(filename);
      UfsDirectoryStatus status = mUnderFileSystem.getDirectoryStatus(filename);
      Assert.assertTrue(exists(PathUtils.concatPath(status.getName())));
    }
  }

  @Test
  public void fileStatus() throws Exception {
    String[] filenames = { "test-dir", "alluxio-fork:///test-dir"};
    for (String filename : filenames) {
      mUnderFileSystem.create(filename).close();
      UfsFileStatus status = mUnderFileSystem.getFileStatus(filename);
      Assert.assertTrue(exists(PathUtils.concatPath(status.getName())));
    }
  }

  private boolean exists(String path) {
    File f = new File(path);
    return f.exists();
  }

  private String normalize(String path) throws Exception {
    URI uri = new URI(path);
    return uri.getPath();
  }
}
