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

package alluxio.master.privilege;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.GetGroupPrivilegesTOptions;
import alluxio.thrift.GetGroupToPrivilegesMappingTOptions;
import alluxio.thrift.GetUserPrivilegesTOptions;
import alluxio.thrift.GrantPrivilegesTOptions;
import alluxio.thrift.PrivilegeMasterClientService;
import alluxio.thrift.RevokePrivilegesTOptions;
import alluxio.thrift.TPrivilege;
import alluxio.wire.ClosedSourceThriftUtils;
import alluxio.wire.Privilege;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Thrift handler for privilege master RPCs invoked by Alluxio clients.
 */
public final class PrivilegeMasterClientServiceHandler
    implements PrivilegeMasterClientService.Iface {
  private static final Logger LOG =
      LoggerFactory.getLogger(PrivilegeMasterClientServiceHandler.class);

  private final PrivilegeMaster mPrivilegeMaster;

  /**
   * @param privilegeMaster the {@link PrivilegeMaster} used to serve RPC requests
   */
  public PrivilegeMasterClientServiceHandler(PrivilegeMaster privilegeMaster) {
    Preconditions.checkNotNull(privilegeMaster);
    mPrivilegeMaster = privilegeMaster;
  }

  @Override
  public long getServiceVersion() throws TException {
    return Constants.PRIVILEGE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public List<TPrivilege> getGroupPrivileges(final String group, GetGroupPrivilegesTOptions options)
      throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<List<TPrivilege>>() {
      @Override
      public List<TPrivilege> call() throws AlluxioException {
        return ClosedSourceThriftUtils.toThrift(mPrivilegeMaster.getGroupPrivileges(group));
      }
    });
  }

  @Override
  public List<TPrivilege> getUserPrivileges(final String user, GetUserPrivilegesTOptions options)
      throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<List<TPrivilege>>() {
      @Override
      public List<TPrivilege> call() throws AlluxioException {
        return ClosedSourceThriftUtils.toThrift(mPrivilegeMaster.getUserPrivileges(user));
      }
    });
  }

  @Override
  public Map<String, List<TPrivilege>> getGroupToPrivilegesMapping(
      GetGroupToPrivilegesMappingTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<Map<String, List<TPrivilege>>>() {
      @Override
      public Map<String, List<TPrivilege>> call() throws AlluxioException {
        Map<String, List<Privilege>> privilegeMap = mPrivilegeMaster.getGroupToPrivilegesMapping();
        Map<String, List<TPrivilege>> tprivilegeMap = new HashMap<>();
        for (Map.Entry<String, List<Privilege>> entry : privilegeMap.entrySet()) {
          tprivilegeMap.put(entry.getKey(), ClosedSourceThriftUtils.toThrift(entry.getValue()));
        }
        return tprivilegeMap;
      }
    });
  }

  @Override
  public List<TPrivilege> grantPrivileges(final String group, final List<TPrivilege> privileges,
      GrantPrivilegesTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<List<TPrivilege>>() {
      @Override
      public List<TPrivilege> call() throws AlluxioException {
        return ClosedSourceThriftUtils.toThrift(mPrivilegeMaster.updatePrivilegesAndJournal(group,
            ClosedSourceThriftUtils.fromThrift(privileges), true));
      }
    });
  }

  @Override
  public List<TPrivilege> revokePrivileges(final String group, final List<TPrivilege> privileges,
      RevokePrivilegesTOptions options) throws AlluxioTException, TException {
    return RpcUtils.call(LOG, new RpcCallable<List<TPrivilege>>() {
      @Override
      public List<TPrivilege> call() throws AlluxioException {
        return ClosedSourceThriftUtils.toThrift(mPrivilegeMaster.updatePrivilegesAndJournal(group,
            ClosedSourceThriftUtils.fromThrift(privileges), false));
      }
    });
  }
}
