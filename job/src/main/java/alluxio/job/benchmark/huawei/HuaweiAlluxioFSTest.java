package alluxio.job.benchmark.huawei;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.WriteType;

public class HuaweiAlluxioFSTest {
  private final String path;
  private final int depth;
  private final int width;
  private final int count;
  private final int size;

  public HuaweiAlluxioFSTest(String path, int depth, int width, int count, int size) {
    this.path = path;
    this.depth = depth;
    this.width = width;
    this.count = count;
    this.size = size;
  }

  private String[] buildPath(String parentPath, int depth) {
    String curPath[] = new String[this.width];

    for (int i = 1; i <= curPath.length; i++) {
      curPath[i - 1] = parentPath + "/vdb." + depth + "_" + i + ".dir";
    }

    return curPath;
  }

  private void recurseFile(String parentPath, FileOperation operation) throws Exception {
    for (int i = 1; i <= this.count; i++) {
      String filePath =
          parentPath + "/vdb_f" + String.format("%0" + String.valueOf(this.count).length() + "d", i)
              + ".file";

      AlluxioURI uri = new AlluxioURI(filePath);
      operation.run(uri);
    }
  }

  private void recursePath(String parentPath, int depth, FileOperation operation) throws Exception {
    String curPath[] = buildPath(parentPath, depth);

    if (depth == this.depth) {
      for (String path : curPath) {
        recurseFile(path, operation);
      }
    } else {
      for (String path : curPath) {
        recursePath(path, depth + 1, operation);
      }
    }
  }

  public void testReadFile(ReadType type) throws Exception {
    recursePath(this.path, 1, new ReadFileOperation(this.size, type));
  }

  public void testWriteFile(WriteType type) throws Exception {
    recursePath(this.path, 1, new WriteFileOperation(this.size, type));
  }

  public static void main(String[] args) {
    System.out.println("test start!!!");

    long startTimeMs = System.currentTimeMillis();

    HuaweiAlluxioFSTest genFSTest =
        new HuaweiAlluxioFSTest(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]),
            Integer.parseInt(args[4]), Integer.parseInt(args[5]));

    try {
      if (args[0].equals("r") || args[0].equals("read")) {
        genFSTest.testReadFile(ReadType.CACHE);
      } else {
        if (args.length == 7 && args[6].equals("sync")) {
          genFSTest.testWriteFile(WriteType.CACHE_THROUGH);
        } else {
          genFSTest.testWriteFile(WriteType.ASYNC_THROUGH);
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    long endTimeMs = System.currentTimeMillis();

    System.out.println("Total cost time: " + (endTimeMs - startTimeMs));
    System.out.println("test done!!!");
  }
}

