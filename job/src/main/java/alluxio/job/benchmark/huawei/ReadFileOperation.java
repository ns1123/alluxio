package alluxio.job.benchmark.huawei;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;

import java.io.IOException;

class ReadFileOperation implements FileOperation {
  private int size;
  private ReadType type;

  public ReadFileOperation(int size, ReadType type) {
    this.size = size;
    this.type = type;
  }

  public void run(AlluxioURI uri) throws FileDoesNotExistException, IOException, AlluxioException {
    OpenFileOptions options = OpenFileOptions.defaults();
    options.setReadType(this.type);

    FileInStream in = null;

    try {
      in = fs.openFile(uri, options);
    } catch (FileDoesNotExistException ex) {
      System.out.println("file " + uri.getPath() + " not exists: " + ex);
      throw ex;
    } catch (Exception ex) {
      System.out.println("open file " + uri.getPath() + " failed: " + ex);
      throw ex;
    }

    try {
      for (int i = 0; i < this.size / 8; i++) {
        in.read(dataBufer.array());
      }
    } catch (IOException ex) {
      System.out.println("read file " + uri.getPath() + " failed: " + ex);
      throw ex;
    } finally {
      in.close();
    }
  }
}

