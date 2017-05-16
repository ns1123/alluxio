package alluxio.underfs.mock;

import alluxio.AlluxioURI;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Mock implementation of {@link alluxio.underfs.UnderFileSystem} used for testing
 * {@link alluxio.underfs.fork.ForkUnderFileSystem}.
 */
public class MockUnderFileSystem implements UnderFileSystem {
  private final UnderFileSystemConfiguration mUfsConf;

  /**
   * Creates a new instance of {@link MockUnderFileSystem}.
   *
   * @param ufsConf the under file system configuration
   */
  public MockUnderFileSystem(UnderFileSystemConfiguration ufsConf) {
    mUfsConf = ufsConf;
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void configureProperties() throws IOException {}

  @Override
  public void connectFromMaster(String hostname) throws IOException {}

  @Override
  public void connectFromWorker(String hostname) throws IOException {}

  @Override
  public OutputStream create(String path) throws IOException {
    return null;
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return null;
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    return false;
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    return false;
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return false;
  }

  @Override
  public boolean exists(String path) throws IOException {
    return false;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return -1;
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return null;
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options) throws IOException {
    return null;
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    return null;
  }

  @Override
  public Map<String, String> getProperties() {
    return mUfsConf.getUserSpecifiedConf();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return -1;
  }

  @Override
  public String getUnderFSType() {
    return "mock";
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return false;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return false;
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    return null;
  }

  @Override
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    return null;
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    return false;
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    return false;
  }

  @Override
  public InputStream open(String path) throws IOException {
    return null;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return null;
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    return false;
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    return false;
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return null;
  }

  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {}

  @Override
  public void setProperties(Map<String, String> properties) {}

  @Override
  public boolean supportsFlush() {
    return false;
  }
}
