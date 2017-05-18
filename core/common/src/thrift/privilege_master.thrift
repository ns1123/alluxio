namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

enum TPrivilege {
  FREE,
  PIN,
  REPLICATION,
  TTL
}

struct GetGroupPrivilegesTOptions {}

struct GetUserPrivilegesTOptions {}

struct GetGroupToPrivilegesMappingTOptions {}

struct GrantPrivilegesTOptions {}

struct RevokePrivilegesTOptions {}

/**
 * This interface contains privilege master service endpoints for Alluxio clients.
 */
service PrivilegeMasterClientService extends common.AlluxioService {

  /**
   * Returns the privilege information for the given group.
   */
  list<TPrivilege> getGroupPrivileges(
    /** the name of the group */ 1: string group,
    /** method options */ 2: GetGroupPrivilegesTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the privilege information for the given user.
   */
  list<TPrivilege> getUserPrivileges(
    /** the name of the user */ 1: string user,
    /** method options */ 2: GetUserPrivilegesTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the mapping from groups to privileges.
   */
  map<string, list<TPrivilege>> getGroupToPrivilegesMapping(
    /** method options */ 1: GetGroupToPrivilegesMappingTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Grants the given privileges to the given group, returning the updated privileges for the group.
   */
  list<TPrivilege> grantPrivileges(
    /** the name of the group */ 1: string group,
    /** the privileges to grant */ 2: list<TPrivilege> privileges,
    /** method options */ 3: GrantPrivilegesTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Removes the given privileges from the given group, returning the updated privileges for the group.
   */
  list<TPrivilege> revokePrivileges(
    /** the name of the group */ 1: string group,
    /** the privileges to revoke */ 2: list<TPrivilege> privileges,
    /** method options */ 3: RevokePrivilegesTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
