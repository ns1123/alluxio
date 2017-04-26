namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

enum Status {
  UNKNOWN = 0,
  CREATED = 1,
  CANCELED = 2,
  FAILED = 3,
  RUNNING = 4,
  COMPLETED = 5,
}

struct TaskInfo {
  1: i64 jobId
  2: i32 taskId
  3: Status status
  4: string errorMessage
  5: binary result
}

struct JobInfo {
  1: i64 id
  2: string errorMessage
  3: list<TaskInfo> taskInfos
  4: Status status
  5: string result
}

union JobCommand {
  1: optional RunTaskCommand runTaskCommand
  2: optional CancelTaskCommand cancelTaskCommand
  3: optional RegisterCommand registerCommand
}

struct CancelTaskCommand {
  1: i64 jobId
  2: i32 taskId
}

struct RegisterCommand {}

struct RunTaskCommand {
  1: i64 jobId
  2: i32 taskId
  3: binary jobConfig
  4: binary taskArgs
}

/**
 * This interface contains job master service endpoints for job service clients.
 */
service JobMasterClientService extends common.AlluxioService {

  /**
   * Cancels the given job.
   */
  void cancel(
    /** the job id */ 1: i64 id,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Gets the status of the given job.
   */
  JobInfo getStatus(
    /** the job id */ 1: i64 id,
    )
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Lists ids of all known jobs.
   */
  list<i64> listAll()
    throws (1: exception.AlluxioTException e)

  /**
   * Starts the given job, returning a job id.
   */
  i64 run(
    /** the command line job info */ 1: binary jobConfig,
    )
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)
}

/**
 * This interface contains job master service endpoints for job service workers.
 */
service JobMasterWorkerService extends common.AlluxioService {

  /**
   * Periodic worker heartbeat returns a list of commands for the worker to execute.
   */
  list<JobCommand> heartbeat(
    /** the id of the worker */ 1: i64 workerId,
    /** the list of tasks status **/ 2: list<TaskInfo> taskInfoList
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns a worker id for the given network address.
   */
  i64 registerWorker(
    /** the worker network address */ 1: common.WorkerNetAddress workerNetAddress
    )
    throws (1: exception.AlluxioTException e)
}
