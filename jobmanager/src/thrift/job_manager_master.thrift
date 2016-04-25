namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

enum Status {
  CREATED,
  CANCELED,
  FAILED,
  RUNNING,
  COMPLETED
}

struct TaskInfo {
  1: i64 jobId
  2: i32 TaskId
  3: Status status
  4: string errorMessage
  5: binary result
}

union JobManangerCommand {
  1: optional RunTaskCommand runTaskCommand
  2: optional CancelTaskCommand cancelTaskCommand
}

struct RunTaskCommand {
  1: i64 jobId
  2: i32 TaskId
  3: binary jobConfig
  4: binary taskArgs
}

struct CancelTaskCommand {
  1: i64 jobId
  2: i32 TaskId
}

/**
 * This interface contains job manager master service endpoints for job manager workers.
 */
service JobManagerMasterWorkerService extends common.AlluxioService {

  /**
   * Returns a worker id for the given network address.
   */
  i64 getWorkerId( /** the worker network address */ 1: common.WorkerNetAddress workerNetAddress)

  /**
   * Periodic worker heartbeat returns a list of commands for the worker to execute.
   */
  list<JobManangerCommand> heartbeat(/** the id of the worker */ 1: i64 workerId,
      /** the list of tasks status **/ 2: list<TaskInfo> taskInfoList)
  throws (1: exception.AlluxioTException e)

    /**
     * Registers a worker.
     */
    void registerWorker( /** the id of the worker */  1: i64 workerId)
      throws (1: exception.AlluxioTException e)
}
