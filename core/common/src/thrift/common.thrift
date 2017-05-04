namespace java alluxio.thrift

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
}

service AlluxioService {

  /**
   * Returns the version of the master service.
   * NOTE: The version should be updated every time a backwards incompatible API change occurs.
   */
  i64 getServiceVersion()
}
