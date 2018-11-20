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

package alluxio.hadoop;

import alluxio.Constants;
import alluxio.util.ConfigurationUtils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * Renewer for Alluxio delegation tokens.
 */
public class AlluxioDelegationTokenRenewer extends TokenRenewer {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioDelegationTokenRenewer.class);

  static {
    // Ensures that HDFS Configuration files are loaded before trying to use
    // the renewer.
    HdfsConfiguration.init();
  }

  @Override
  public boolean handleKind(Text kind) {
    return AlluxioDelegationTokenIdentifier.ALLUXIO_DELEGATION_KIND.equals(kind);
  }

  @Override
  public long renew(Token<?> token, Configuration conf) throws IOException {
    Preconditions.checkNotNull(token, "token");
    Preconditions.checkArgument(this.handleKind(token.getKind()),
        String.format("Could not renew token kind %s", token.getKind()));
    Token<AlluxioDelegationTokenIdentifier> alluxioToken =
        (Token<AlluxioDelegationTokenIdentifier>) token;
    LOG.debug("Renewing Token {}", alluxioToken);
    FileSystem fileSystem = getFileSystem(alluxioToken, conf);
    return fileSystem.renewDelegationToken(alluxioToken);
  }

  @Override
  public void cancel(Token<?> token, Configuration conf) throws IOException {
    Preconditions.checkNotNull(token, "token");
    Preconditions.checkArgument(this.handleKind(token.getKind()),
        String.format("Could not cancel token kind %s", token.getKind()));
    Token<AlluxioDelegationTokenIdentifier> alluxioToken =
        (Token<AlluxioDelegationTokenIdentifier>) token;
    LOG.debug("Cancelling Token {}", alluxioToken);
    FileSystem fileSystem = getFileSystem(alluxioToken, conf);
    fileSystem.cancelDelegationToken(alluxioToken);
  }

  /**
   * Gets an Alluxio file system client corresponding to the token.
   * @param token the token to get a file system client for
   * @param conf the HDFS configuration for the client
   * @return an Alluxio file system client that can be used for managing the token
   * @throws IOException if error occurred while getting the file system
   */
  private static alluxio.hadoop.FileSystem getFileSystem(
      Token<AlluxioDelegationTokenIdentifier> token, Configuration conf)
      throws IOException {
    Preconditions.checkNotNull(token, "token");
    String serviceName = token.getService().toString();
    String authority = serviceName;
    if (serviceName.startsWith(Constants.HEADER) || serviceName.startsWith(Constants.HEADER_FT)) {
      HadoopConfigurationUtils.mergeHadoopConfiguration(conf, alluxio.Configuration.global());
      if (!ConfigurationUtils.masterHostConfigured()) {
        // The token is for Alluxio in HA mode but we don't have enough information to determine
        // master address.
        throw new IOException(String.format("Cannot determine master addresses for managing token %s",
            token.toString()));
      }
      try {
        authority = new URI(serviceName).getAuthority();
      } catch (URISyntaxException e) {
        throw new IOException(String.format("Invalid service name from delegation token %s",
            token.toString()), e);
      }
    }
    try {
      FileSystem fs = new FileSystem();
      // If no authority is given (e.g. "alluxio:///"), add a trailing slash so URI parsing does not
      // panic on incomplete URI like "alluxio://".
      fs.initialize(new URI(Constants.HEADER + Objects.toString(authority, "/")), conf);
      return fs;
    } catch (URISyntaxException e) {
      throw new IOException(String.format("Invalid service authority from delegation token %s",
          token.toString()), e);
    }
  }

  @Override
  public boolean isManaged(Token<?> token) {
    return true;
  }
}
