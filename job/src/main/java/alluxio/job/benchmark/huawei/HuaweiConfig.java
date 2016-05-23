package alluxio.job.benchmark.huawei;

import alluxio.job.benchmark.AbstractBenchmarkJobConfig;
import alluxio.job.benchmark.huawei.HuaweiAlluxioFSTest;

public class HuaweiConfig extends AbstractBenchmarkJobConfig {
  private static final long serialVersionUID = 7162741309362058756L;
  public static String NAME = "HUAWEI";

  private int mDepth;
  private int mWidth;
  private int mCount;
  private int mSize;

  /**
   * Creates a new instance of {@link HuaweiConfig}.
   * @param depth the depth of the file system to test
   * @param width the number of files in a directory
   * @param count the number of files in total
   * @param size the file size
   * @param cleanUp run clean up after test if set to true
   */
  public HuaweiConfig(int depth, int width, int count, int size, boolean cleanUp) {
    super(1, 1, "ALLUXIO", true, cleanUp);
    mDepth = depth;
    mWidth = width;
    mCount = count;
    mSize = size;
  }

  int getDepth() {
    return mDepth;
  }

  int getWidth() {
    return mWidth;
  }

  @Override
  public String getName() {
    return NAME;
  }
}