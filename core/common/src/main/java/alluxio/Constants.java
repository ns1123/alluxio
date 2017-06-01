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

package alluxio;

import javax.annotation.concurrent.ThreadSafe;

/**
 * System wide constants.
 */
@ThreadSafe
public final class Constants {
  public static final int KB = 1024;
  public static final int MB = KB * 1024;
  public static final int GB = MB * 1024;
  public static final long TB = GB * 1024L;
  public static final long PB = TB * 1024L;

  public static final long SECOND = 1000;
  public static final long MINUTE = SECOND * 60L;
  public static final long HOUR = MINUTE * 60L;
  public static final long DAY = HOUR * 24L;

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";

  public static final String LS_FORMAT_PERMISSION = "%-15s";
  public static final String LS_FORMAT_FILE_SIZE = "%-10s";
  public static final String LS_FORMAT_CREATE_TIME = "%-25s";
  public static final String LS_FORMAT_FILE_TYPE = "%-15s";
  public static final String LS_FORMAT_USER_NAME = "%-15s";
  public static final String LS_FORMAT_GROUP_NAME = "%-15s";
  public static final String LS_FORMAT_FILE_PATH = "%-5s";
  public static final String LS_FORMAT = LS_FORMAT_PERMISSION + LS_FORMAT_USER_NAME
      + LS_FORMAT_GROUP_NAME + LS_FORMAT_FILE_SIZE + LS_FORMAT_CREATE_TIME + LS_FORMAT_FILE_TYPE
      + LS_FORMAT_FILE_PATH + "%n";
  public static final String LS_FORMAT_NO_ACL = LS_FORMAT_FILE_SIZE + LS_FORMAT_CREATE_TIME
      + LS_FORMAT_FILE_TYPE + LS_FORMAT_FILE_PATH + "%n";

  public static final String MESOS_RESOURCE_CPUS = "cpus";
  public static final String MESOS_RESOURCE_MEM = "mem";
  public static final String MESOS_RESOURCE_DISK = "disk";
  public static final String MESOS_RESOURCE_PORTS = "ports";

  public static final long SECOND_NANO = 1000000000L;
  public static final int SECOND_MS = 1000;
  public static final int MINUTE_MS = SECOND_MS * 60;
  public static final int HOUR_MS = MINUTE_MS * 60;
  public static final int DAY_MS = HOUR_MS * 24;

  public static final int BYTES_IN_INTEGER = 4;

  public static final long UNKNOWN_SIZE = -1;

  public static final String SCHEME = "alluxio";
  public static final String HEADER = SCHEME + "://";

  public static final String SCHEME_FT = "alluxio-ft";
  public static final String HEADER_FT = SCHEME_FT + "://";

  public static final String HEADER_OSS = "oss://";

  public static final String HEADER_S3 = "s3://";
  public static final String HEADER_S3N = "s3n://";
  public static final String HEADER_S3A = "s3a://";
  public static final String HEADER_SWIFT = "swift://";
  // ALLUXIO CS ADD
  public static final String HEADER_JDBC = "jdbc:";
  // ALLUXIO CS END
  // Google Cloud Storage header convention is "gs://".
  // See https://cloud.google.com/storage/docs/cloud-console
  public static final String HEADER_GCS = "gs://";

  public static final int MAX_PORT = 65535;

  // Service versions should be incremented every time a backwards incompatible change occurs.
  public static final long BLOCK_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long BLOCK_MASTER_WORKER_SERVICE_VERSION = 2;
  public static final long BLOCK_WORKER_CLIENT_SERVICE_VERSION = 2;
  public static final long FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION = 2;
  // ALLUXIO CS ADD
  public static final long FILE_SYSTEM_MASTER_JOB_SERVICE_VERSION = 2;
  // ALLUXIO CS END
  public static final long FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION = 2;
  public static final long FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION = 2;
  public static final long LINEAGE_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long META_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION = 2;
  public static final long KEY_VALUE_WORKER_SERVICE_VERSION = 2;
  public static final long UNKNOWN_SERVICE_VERSION = -1;

  public static final String BLOCK_MASTER_NAME = "BlockMaster";
  public static final String FILE_SYSTEM_MASTER_NAME = "FileSystemMaster";
  public static final String LINEAGE_MASTER_NAME = "LineageMaster";
  public static final String KEY_VALUE_MASTER_NAME = "KeyValueMaster";
  public static final String BLOCK_WORKER_NAME = "BlockWorker";
  public static final String FILE_SYSTEM_WORKER_NAME = "FileSystemWorker";
  public static final String KEY_VALUE_WORKER_NAME = "KeyValueWorker";

  public static final String BLOCK_MASTER_CLIENT_SERVICE_NAME = "BlockMasterClient";
  public static final String BLOCK_MASTER_WORKER_SERVICE_NAME = "BlockMasterWorker";
  public static final String FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME = "FileSystemMasterClient";
  // ALLUXIO CS ADD
  public static final String FILE_SYSTEM_MASTER_JOB_SERVICE_NAME = "FileSystemMasterJob";
  // ALLUXIO CS END
  public static final String FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME = "FileSystemMasterWorker";
  public static final String LINEAGE_MASTER_CLIENT_SERVICE_NAME = "LineageMasterClient";
  public static final String META_MASTER_SERVICE_NAME = "MetaMaster";
  public static final String BLOCK_WORKER_CLIENT_SERVICE_NAME = "BlockWorkerClient";
  public static final String FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME = "FileSystemWorkerClient";
  public static final String KEY_VALUE_MASTER_CLIENT_SERVICE_NAME = "KeyValueMasterClient";
  public static final String KEY_VALUE_WORKER_CLIENT_SERVICE_NAME = "KeyValueWorkerClient";

  public static final String REST_API_PREFIX = "/api/v1";

  public static final String MASTER_COLUMN_FILE_PREFIX = "COL_";

  public static final String SWIFT_AUTH_KEYSTONE = "keystone";
  public static final String SWIFT_AUTH_KEYSTONE_V3 = "keystonev3";
  public static final String SWIFT_AUTH_SWIFTAUTH = "swiftauth";

  public static final String MESOS_LOCAL_INSTALL = "LOCAL";

  /**
   * Maximum number of seconds to wait for thrift servers to stop on shutdown. Tests use a value of
   * 0 instead of this value so that they can run faster.
   */
  public static final int THRIFT_STOP_TIMEOUT_SECONDS = 60;

  // Time-to-live
  public static final long NO_TTL = -1;

  // Security
  public static final int DEFAULT_FILE_SYSTEM_UMASK = 0022;
  public static final short DEFAULT_FILE_SYSTEM_MODE = (short) 0777;
  public static final short FILE_DIR_PERMISSION_DIFF = (short) 0111;
  public static final short INVALID_MODE = -1;

  // Specific tier write
  public static final int FIRST_TIER = 0;
  public static final int SECOND_TIER = 1;
  public static final int LAST_TIER = -1;

  // ALLUXIO CS ADD
  // Replication
  public static final int REPLICATION_MAX_INFINITY = -1;

  // Persistence
  public static final int PERSISTENCE_INVALID_JOB_ID = -1;
  public static final String PERSISTENCE_INVALID_UFS_PATH = "";

  // Job service
  public static final String JOB_MASTER_WORKER_SERVICE_NAME = "JobMasterWorker";
  public static final long JOB_MASTER_WORKER_SERVICE_VERSION = 1;
  public static final String JOB_MASTER_NAME = "JobMaster";
  public static final String JOB_MASTER_CLIENT_SERVICE_NAME = "JobMasterClient";
  public static final int JOB_MASTER_CLIENT_SERVICE_VERSION = 1;
  public static final String JOB_WORKER_NAME = "JobWorker";

  public static final int JOB_DEFAULT_MASTER_PORT = 20001;
  public static final int JOB_DEFAULT_MASTER_WEB_PORT = JOB_DEFAULT_MASTER_PORT + 1;
  public static final int JOB_DEFAULT_WORKER_PORT = 30001;
  public static final int JOB_DEFAULT_WORKER_DATA_PORT = JOB_DEFAULT_WORKER_PORT + 1;
  public static final int JOB_DEFAULT_WORKER_WEB_PORT = JOB_DEFAULT_WORKER_PORT + 2;

  // Call home
  public static final String CALL_HOME_MASTER_NAME = "CallHomeMaster";

  // License checking
  public static final String LICENSE_MASTER_NAME = "LicenseMaster";

  // Privilege checking
  public static final String PRIVILEGE_MASTER_NAME = "PrivilegeMaster";
  public static final String PRIVILEGE_MASTER_CLIENT_SERVICE_NAME = "PrivilegeMasterClient";
  public static final long PRIVILEGE_MASTER_CLIENT_SERVICE_VERSION = 1;

  // Kerberos
  public static final String KERBEROS_DEFAULT_AUTH_TO_LOCAL = "DEFAULT";

  // Encryption
  public static final String AES_GCM_NOPADDING = "AES/GCM/NoPadding";
  public static final long DEFAULT_CHUNK_SIZE = 64 * KB;
  public static final long DEFAULT_CHUNK_FOOTER_SIZE = 16L;
  public static final String ENCRYPTION_MAGIC = "ALLUXIO1";
  public static final long INVALID_ENCRYPTION_ID = -1L;
  public static final String KMS_DUMMY_PROVIDER_NAME = "DUMMY";
  public static final String KMS_HADOOP_PROVIDER_NAME = "HADOOP";
  public static final String KMS_TS_PROVIDER_NAME = "TS";
  public static final String KMS_API_PREFIX = "";
  // For dummy KMS
  public static final String ENCRYPTION_KEY_FOR_TESTING = "16bytesSecretKey";
  public static final String ENCRYPTION_IV_FOR_TESTING = "iv";

  // Native library
  public static final String NATIVE_ALLUXIO_LIB_NAME = "alluxio";

  // ALLUXIO CS END
  private Constants() {} // prevent instantiation
}
