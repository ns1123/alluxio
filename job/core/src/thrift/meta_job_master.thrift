namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

enum JobMasterInfoField {
  WEB_PORT
}

struct JobMasterInfo {
  1: i32 webPort
}

/**
 * This interface contains meta job master service endpoints.
 */
service MetaJobMasterClientService extends common.AlluxioService {

/**
   * Returns information about the job master.
   */
  JobMasterInfo getInfo( /**
      /** optional filter for what fields to return, defaults to all */ 1: set<JobMasterInfoField> fields,
  )
}
