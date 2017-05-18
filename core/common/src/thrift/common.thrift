namespace java alluxio.thrift

include "exception.thrift"

/**
 * Contains the information of a block in Alluxio. It maintains the worker nodes where the replicas
 * of the blocks are stored.
 */
struct BlockInfo {
  1: i64 blockId
  2: i64 length
  3: list<BlockLocation> locations
}

/**
 * Information about blocks.
 */
struct BlockLocation {
  1: i64 workerId
  2: WorkerNetAddress workerAddress
  3: string tierAlias
}

enum CommandType {
  Unknown = 0,
  Nothing = 1,
  Register = 2, // Ask the worker to re-register.
  Free = 3,     // Ask the worker to free files.
  Delete = 4,   // Ask the worker to delete files.
  Persist = 5,  // Ask the worker to persist a file for lineage
}

enum TTtlAction {
  Delete = 0, // Delete the file after TTL expires.
  Free = 1,   // Free the file after TTL expires.
}

struct Command {
  1: CommandType commandType
  2: list<i64> data
}

/**
 * Address information about workers.
 */
struct WorkerNetAddress {
  1: string host
  2: i32 rpcPort
  3: i32 dataPort
  4: i32 webPort
  5: string domainSocketPath
  // ALLUXIO CS ADD
  1001: i32 secureRpcPort
  // ALLUXIO CS END
}

<<<<<<< HEAD
// ALLUXIO CS ADD
struct Capability {
  1: optional binary content
  2: optional binary authenticator
  3: optional i64 keyId
}
// ALLUXIO CS END
/**
* Information about the RPC.
*/
struct RpcOptions {
  // key used to identify retried RPCs
  1: optional string key
||||||| merged common ancestors
/**
* Information about the RPC.
*/
struct RpcOptions {
  // key used to identify retried RPCs
  1: optional string key
=======
struct GetServiceVersionTResponse {
  1: i64 version,
>>>>>>> bbee51dd64c2302a9fca29b05d4ccfab1298a074
}

struct GetServiceVersionTOptions {}

service AlluxioService {

  /**
   * Returns the version of the master service.
   * NOTE: The version should be updated every time a backwards incompatible API change occurs.
   */
  GetServiceVersionTResponse getServiceVersion(
    /** the method options */ 1: GetServiceVersionTOptions options,
  ) throws (1: exception.AlluxioTException e)
}
