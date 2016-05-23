package alluxio.job.benchmark.huawei;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;

import java.nio.ByteBuffer;

interface FileOperation {
  int buffSize = 8192;
  ByteBuffer dataBufer = ByteBuffer.allocate(buffSize);
  FileSystem fs = FileSystem.Factory.get();

  void run(AlluxioURI uri) throws Exception;
}
