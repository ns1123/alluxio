package alluxio.job.benchmark;

import alluxio.AlluxioURI;
import alluxio.client.ClientContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.FileSystemMasterClientPool;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.job.JobWorkerContext;

import java.io.IOException;

public class FSMasterCreatePathDefinition
    extends AbstractThroughputLatencyJobDefinition<FSMasterCreatePathConfig> {
  private FileSystemMasterClientPool mFileSystemMasterClientPool = null;

  private int[] mProducts;

  /**
   * Creates FSMasterCreateDirDefinition instance.
   */
  public FSMasterCreatePathDefinition() {}

  @Override
  protected void before(FSMasterCreatePathConfig config, JobWorkerContext jobWorkerContext)
      throws Exception {
    super.before(config, jobWorkerContext);
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(ClientContext.getMasterAddress(), config.getThreadNum());
    // Precompute this to save some CPU.
    mProducts = new int[config.getLevel()];
    mProducts[config.getLevel() - 1] = 1;
    for (int i = config.getLevel() - 2; i >= 0; i--) {
      mProducts[i] = mProducts[i + 1] * config.getDirSize();
    }
  }

  @Override
  protected boolean execute(FSMasterCreatePathConfig config, JobWorkerContext jobWorkerContext,
      int commandId) {
    FileSystemMasterClient client = mFileSystemMasterClientPool.acquire();
    try {
      String path = constructPathFromCommandId(commandId);
      if (config.isDirectory()) {
        client.createDirectory(new AlluxioURI(path),
            CreateDirectoryOptions.defaults().setRecursive(true).setAllowExists(true));
      } else {
        client.createFile(new AlluxioURI(path), CreateFileOptions.defaults().setRecursive(true));
        client.completeFile(new AlluxioURI(path), CompleteFileOptions.defaults());
      }
    } catch (AlluxioException e) {
      LOG.warn("Directory creation failed: ", e);
      return false;
    } catch (IOException e) {
      LOG.warn("Directory creation failed: ", e);
      return false;
    } finally {
      mFileSystemMasterClientPool.release(client);
    }
    return true;
  }

  /**
   * Constructs the file path to given a commandId.
   *
   * @param commandId the commandId. Each commandId corresponds to one file.
   * @return the file path to create
   */
  private String constructPathFromCommandId(int commandId) {
    StringBuilder path = new StringBuilder("/");
    for (int i = 0; i < mProducts.length; i++) {
      path.append(commandId / mProducts[i]);
      if (i != mProducts.length - 1) {
        path.append("/");
      }
      commandId %= mProducts[i];
    }
    return path.toString();
  }
}