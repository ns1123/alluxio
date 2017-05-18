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

package alluxio.client.privilege;

import alluxio.AbstractMasterClient;
import alluxio.Constants;
import alluxio.client.file.RetryHandlingFileSystemMasterClient;
import alluxio.client.privilege.options.GetGroupPrivilegesOptions;
import alluxio.client.privilege.options.GetGroupToPrivilegesMappingOptions;
import alluxio.client.privilege.options.GetUserPrivilegesOptions;
import alluxio.client.privilege.options.GrantPrivilegesOptions;
import alluxio.client.privilege.options.RevokePrivilegesOptions;
import alluxio.thrift.AlluxioService.Client;
import alluxio.thrift.PrivilegeMasterClientService;
import alluxio.thrift.TPrivilege;
import alluxio.wire.ClosedSourceThriftUtils;
import alluxio.wire.Privilege;

import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A wrapper for the thrift client to interact with the privilege master, used by Alluxio clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
@ThreadSafe
public final class RetryHandlingPrivilegeMasterClient extends AbstractMasterClient
    implements PrivilegeMasterClient {

  private PrivilegeMasterClientService.Client mClient = null;

  /**
   * Creates a new {@link RetryHandlingFileSystemMasterClient} instance.
   *
   * @param subject the subject
   * @param masterAddress the master address
   */
  protected static RetryHandlingPrivilegeMasterClient create(Subject subject,
      InetSocketAddress masterAddress) {
    return new RetryHandlingPrivilegeMasterClient(subject, masterAddress);
  }

  private RetryHandlingPrivilegeMasterClient(Subject subject, InetSocketAddress masterAddress) {
    super(subject, masterAddress);
  }

  @Override
  public synchronized List<Privilege> getGroupPrivileges(final String group,
      final GetGroupPrivilegesOptions options) throws IOException {
    return retryRPC(new RpcCallable<List<Privilege>>() {
      @Override
      public List<Privilege> call() throws TException {
        return ClosedSourceThriftUtils
            .fromThrift(mClient.getGroupPrivileges(group, options.toThrift()).getPrivileges());
      }
    });
  }

  @Override
  public synchronized List<Privilege> getUserPrivileges(final String user,
      final GetUserPrivilegesOptions options) throws IOException {
    return retryRPC(new RpcCallable<List<Privilege>>() {
      @Override
      public List<Privilege> call() throws TException {
        return ClosedSourceThriftUtils
            .fromThrift(mClient.getUserPrivileges(user, options.toThrift()).getPrivileges());
      }
    });
  }

  @Override
  public synchronized Map<String, List<Privilege>> getGroupToPrivilegesMapping(
      final GetGroupToPrivilegesMappingOptions options) throws IOException {
    return retryRPC(new RpcCallable<Map<String, List<Privilege>>>() {
      @Override
      public Map<String, List<Privilege>> call() throws TException {
        Map<String, List<Privilege>> groupInfo = new HashMap<>();
        for (Map.Entry<String, List<TPrivilege>> entry : mClient
            .getGroupToPrivilegesMapping(options.toThrift()).getGroupPrivilegesMap().entrySet()) {
          groupInfo.put(entry.getKey(), ClosedSourceThriftUtils.fromThrift(entry.getValue()));
        }
        return groupInfo;
      }
    });
  }

  @Override
  public synchronized List<Privilege> grantPrivileges(final String group,
      final List<Privilege> privileges, final GrantPrivilegesOptions options) throws IOException {
    return retryRPC(new RpcCallable<List<Privilege>>() {
      @Override
      public List<Privilege> call() throws TException {
        return ClosedSourceThriftUtils.fromThrift(mClient.grantPrivileges(group,
            ClosedSourceThriftUtils.toThrift(privileges), options.toThrift()).getPrivileges());
      }
    });
  }

  @Override
  public synchronized List<Privilege> revokePrivileges(final String group,
      final List<Privilege> privileges, final RevokePrivilegesOptions options) throws IOException {
    return retryRPC(new RpcCallable<List<Privilege>>() {
      @Override
      public List<Privilege> call() throws TException {
        return ClosedSourceThriftUtils.fromThrift(mClient.revokePrivileges(group,
            ClosedSourceThriftUtils.toThrift(privileges), options.toThrift()).getPrivileges());
      }
    });
  }

  @Override
  protected Client getClient() {
    return mClient;
  }

  @Override
  protected String getServiceName() {
    return Constants.PRIVILEGE_MASTER_CLIENT_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.PRIVILEGE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = new PrivilegeMasterClientService.Client(mProtocol);
  }
}
