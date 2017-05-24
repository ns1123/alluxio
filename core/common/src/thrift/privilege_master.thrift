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
struct GetGroupPrivilegesTResponse {
  1: list<TPrivilege> privileges
}

struct GetUserPrivilegesTOptions {}
struct GetUserPrivilegesTResponse {
  1: list<TPrivilege> privileges
}

struct GetGroupToPrivilegesMappingTOptions {}
struct GetGroupToPrivilegesMappingTResponse {
  1: map<string, list<TPrivilege>> groupPrivilegesMap
}

struct GrantPrivilegesTOptions {}
struct GrantPrivilegesTResponse {
  1: list<TPrivilege> privileges
}

struct RevokePrivilegesTOptions {}
struct RevokePrivilegesTResponse {
  1: list<TPrivilege> privileges
}

/**
 * This interface contains privilege master service endpoints for Alluxio clients.
 */
service PrivilegeMasterClientService extends common.AlluxioService {

  /**
   * Returns the privilege information for the given group.
   */
  GetGroupPrivilegesTResponse getGroupPrivileges(
    /** the name of the group */ 1: string group,
    /** the method options */ 2: GetGroupPrivilegesTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the privilege information for the given user.
   */
  GetUserPrivilegesTResponse getUserPrivileges(
    /** the name of the user */ 1: string user,
    /** the method options */ 2: GetUserPrivilegesTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the mapping from groups to privileges.
   */
  GetGroupToPrivilegesMappingTResponse getGroupToPrivilegesMapping(
    /** the method options */ 1: GetGroupToPrivilegesMappingTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Grants the given privileges to the given group, returning the updated privileges for the group.
   */
  GrantPrivilegesTResponse grantPrivileges(
    /** the name of the group */ 1: string group,
    /** the privileges to grant */ 2: list<TPrivilege> privileges,
    /** the method options */ 3: GrantPrivilegesTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Removes the given privileges from the given group, returning the updated privileges for the group.
   */
  RevokePrivilegesTResponse revokePrivileges(
    /** the name of the group */ 1: string group,
    /** the privileges to revoke */ 2: list<TPrivilege> privileges,
    /** the method options */ 3: RevokePrivilegesTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
