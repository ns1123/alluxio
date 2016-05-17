package  alluxio.job.benchmark;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class FSMasterCreatePathConfig extends AbstractThroughputLatencyJobConfig {
  private static final long serialVersionUID = 7859013978084941882L;

  public static final String NAME = "FSMasterCreateFile";

  private int mLevel;
  private int mDirSize;
  private boolean mIsDirectory;

  /**
   * Creates an instance of AbstractThroughputAndLatencyJobConfig.
   *
   * @param level the number of lvels of the file system tree
   * @param dirSize the number of files or directories each non-leaf directory has
   * @param isDirectory whether the leaves are directories or files
   * @param expectedThroughput the expected throughput
   * @param threadNum the number of client threads
   * @param cleanUp whether to clean up after the test
   */
  public FSMasterCreatePathConfig(@JsonProperty("level") int level,
      @JsonProperty("dirSize") int dirSize,
      @JsonProperty("isDirectory") boolean isDirectory,
      @JsonProperty("throughput") double expectedThroughput,
      @JsonProperty("threadNum") int threadNum,
      @JsonProperty("cleanUp") boolean cleanUp) {
    super((int) Math.pow(dirSize, level), expectedThroughput, threadNum,
        FileSystemType.ALLUXIO, true, cleanUp);
    mDirSize = dirSize;
    mLevel = level;
    mIsDirectory = isDirectory;
  }

  /**
   * @return the level size
   */
  public int getLevel() {
    return mLevel;
  }

  /**
   * @return the directory size
   */
  public int getDirSize() {
    return mDirSize;
  }

  /**
   * @return true if the leaves are directories
   */
  public boolean isDirectory() {
    return mIsDirectory;
  }

  @Override
  public String getName() {
    return NAME;
  }
}