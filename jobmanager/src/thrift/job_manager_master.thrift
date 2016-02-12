namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

enum Status {
  CANCELED,
  ERROR,
  INPROGRESS,
  SUCCESS
}

struct TaskStatus {
  1: i64 jobId
  2: i32 TaskId
  3: Status status
  4: string errorMessage
}

union JobManangerCommand {
  1: optional RunTaskCommand runTaskCommand 
  2: optional CancelTaskCommand cancelTaskCommand
}

struct RunTaskCommand {
  1: i64 jobId
  2: i32 TaskId
  3: string jobName
  4: binary jobConfig
  5: binary taskArgs  
}

struct CancelTaskCommand {
  1: i64 jobId
  2: i32 TaskId  
}

/**
 * This interface contains job manager master service endpoints for job manager workers.
 */
service JobManagerMasterWorkerService extends common.AlluxioService {
  list<JobManangerCommand> heartbeat(/** the id of the worker */ 1: i64 workerId,
      /** the list of tasks status **/ 2: list<TaskStatus> taskStatusList)
  throws (1: exception.AlluxioTException e)
}