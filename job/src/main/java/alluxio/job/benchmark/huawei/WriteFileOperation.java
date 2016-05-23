package alluxio.job.benchmark.huawei;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;

import java.io.IOException;

/**
 * Write file operation definition.
 */
class WriteFileOperation implements FileOperation {
  private int size;
  private WriteType type;

  /**
   * Creates a {@link WriteFileOperation} instance.
   *
   * @param size the file size
   * @param type the write type
   */
  public WriteFileOperation(int size, WriteType type) {
    this.size = size;
    this.type = type;
  }

  @Override
  public void run(AlluxioURI uri) throws IOException, AlluxioException {
    CreateFileOptions options = CreateFileOptions.defaults();
    options.setWriteType(this.type);
    options.setRecursive(true);

    FileOutStream out = null;

    try {
      out = fs.createFile(uri, options);
    } catch (FileAlreadyExistsException ex) {
      System.out.println("file " + uri.getPath() + " already exists: " + ex);
      throw ex;
    } catch (Exception ex) {
      System.out.println("create file " + uri.getPath() + " failed: " + ex);
      throw ex;
    }

    try {
      for (int i = 0; i < this.size / 8; i++) {
        out.write(dataBufer.array());
      }
    } catch (IOException ex) {
      System.out.println("write file " + uri.getPath() + " failed: " + ex);
      throw ex;
    } finally {
      out.close();
    }
  }
}

