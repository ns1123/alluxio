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

package alluxio.server.auth;

import static org.junit.Assert.assertTrue;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Source;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.hadoop.AlluxioDelegationTokenIdentifier;
import alluxio.hadoop.HadoopClientTestUtils;
import alluxio.hadoop.HadoopKerberosLoginProvider;
import alluxio.master.MultiMasterLocalAlluxioCluster;
import alluxio.security.LoginUser;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthType;
import alluxio.security.minikdc.MiniKdc;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

public class DelegationTokenFaultToleranceIntegrationTest extends BaseIntegrationTest {
  // Fail if the cluster doesn't come up after this amount of time.
  private static final int CLUSTER_WAIT_TIMEOUT_MS = 120 * Constants.SECOND_MS;
  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final int BLOCK_SIZE = 30;
  private static final int MASTERS = 5;

  private MultiMasterLocalAlluxioCluster mMultiMasterLocalAlluxioCluster = null;

  private static final String HOSTNAME = NetworkAddressUtils.getLocalHostName();
  private static final String UNIFIED_INSTANCE = "instance";

  private static MiniKdc sKdc;
  private static File sWorkDir;

  private static String sServerPrincipal;
  private static File sServerKeytab;

  @ClassRule
  public static final TemporaryFolder FOLDER = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  public void initKrb() throws Exception {
    sWorkDir = FOLDER.getRoot();
    sKdc = new MiniKdc(MiniKdc.createConf(), sWorkDir);
    sKdc.start();

    String realm = sKdc.getRealm();

    sServerPrincipal = "alluxio/" + UNIFIED_INSTANCE + "@" + realm;
    sServerKeytab = new File(sWorkDir, "alluxio.keytab");
    // Create a principal in miniKDC, and generate the keytab file for it.
    sKdc.createPrincipal(sServerKeytab, "alluxio/" + UNIFIED_INSTANCE);

    Configuration.merge(ImmutableMap.<PropertyKey, Object>builder()
        .put(PropertyKey.MASTER_HOSTNAME, HOSTNAME)
        .put(PropertyKey.WORKER_HOSTNAME, HOSTNAME)
        .put(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.KERBEROS.getAuthName())
        .put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true")
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_PRINCIPAL, sServerPrincipal)
        .put(PropertyKey.SECURITY_KERBEROS_SERVER_KEYTAB_FILE, sServerKeytab.getPath())
        .put(PropertyKey.SECURITY_KERBEROS_SERVICE_NAME, "alluxio")
        .put(PropertyKey.SECURITY_KERBEROS_UNIFIED_INSTANCE_NAME, UNIFIED_INSTANCE)
        .build(), Source.RUNTIME);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sKdc != null) {
      sKdc.stop();
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Skip hadoop 1 because hadoop 1's RPC cannot be interrupted properly which makes it
    // hard to shutdown a cluster.
    // TODO(peis): Figure out a better way to support hadoop 1.
    Assume.assumeFalse(HadoopClientTestUtils.isHadoop1x());
  }

  @After
  public final void after() throws Exception {
    mMultiMasterLocalAlluxioCluster.stop();

    LoginUserTestUtils.resetLoginUser();
  }

  @Before
  public final void before() throws Exception {
    LoginUserTestUtils.resetLoginUser();
    mMultiMasterLocalAlluxioCluster =
        new MultiMasterLocalAlluxioCluster(MASTERS);
    mMultiMasterLocalAlluxioCluster.initConfiguration();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES);
    Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE);
    Configuration.set(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 100);
    Configuration.set(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, 2);
    Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, 32);
    initKrb();
    mMultiMasterLocalAlluxioCluster.start();
  }

  @Test
  public void getDelegationTokensFault() throws Exception {
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL, sServerPrincipal);
    Configuration.set(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE, sServerKeytab.getPath());
    UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("alluxio");
    UserGroupInformation.setLoginUser(tokenUGI);
    Credentials creds = new Credentials();
    boolean isConnected = true;
    try {
      URI uri = new URI(Constants.HEADER + "alluxio_service/");
      alluxio.client.file.FileSystem fs = mMultiMasterLocalAlluxioCluster.getClient();
      org.apache.hadoop.fs.FileSystem hdfs = new alluxio.hadoop.FileSystem(fs);
      org.apache.hadoop.conf.Configuration hdfsConf = new org.apache.hadoop.conf.Configuration();
      hdfs.initialize(uri, hdfsConf);
      // obtains delegation token
      Token<?>[] tokens = hdfs.addDelegationTokens("alluxio", creds);
      Assert.assertEquals(1, tokens.length);
      Assert.assertEquals(
          AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND,
          tokens[0].getKind());

      tokens[0].renew(hdfsConf);

      LoginUser.reset();
      tokenUGI.addCredentials(creds);

      // accesses master using delegation token
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_PRINCIPAL);
      Configuration.unset(PropertyKey.SECURITY_KERBEROS_CLIENT_KEYTAB_FILE);
      LoginUser.setExternalLoginProvider(new HadoopKerberosLoginProvider());

      // kills leaders one by one and uses delegation token after each fail over
      for (int kills = 0; kills < MASTERS - 1; kills++) {
        assertTrue(mMultiMasterLocalAlluxioCluster.stopLeader());
        mMultiMasterLocalAlluxioCluster.waitForNewMaster(CLUSTER_WAIT_TIMEOUT_MS);

        // gets a new client
        mMultiMasterLocalAlluxioCluster.getLocalAlluxioMaster().clearClients();
        FileSystemContext.clearCache();
        fs = mMultiMasterLocalAlluxioCluster.getClient();
        alluxio.hadoop.FileSystem newHdfs = new alluxio.hadoop.FileSystem(fs);

        tokenUGI.doAs((PrivilegedExceptionAction<? extends Object>) () -> {
          newHdfs.initialize(uri, hdfsConf);
          Path rootPath = new Path("/");
          FileStatus rootStat = newHdfs.getFileStatus(rootPath);
          Assert.assertEquals(rootStat.getPath().getName(), rootPath.getName());
          return null;
        });
      }
    } catch (UnauthenticatedException | UnavailableException e) {
      isConnected = false;
    }
    Assert.assertTrue(isConnected);
  }
}
