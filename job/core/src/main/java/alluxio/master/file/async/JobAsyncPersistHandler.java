/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.master.file.async;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.job.util.JobRestClientUtils;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.job.JobMasterClientRestServiceHandler;
import alluxio.thrift.PersistFile;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;

/**
 * The async persist handler that schedules the async persistence of file by sending a persistence
 * request to the job service.
 */
public final class JobAsyncPersistHandler implements AsyncPersistHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Constructs a new instance of {@link JobAsyncPersistHandler}.
   *
   * @param view the view of {@link FileSystemMasterView}
   */
  public JobAsyncPersistHandler(FileSystemMasterView view) {}

  @Override
  public synchronized void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException {
    InetSocketAddress address = JobRestClientUtils.getJobMasterAddress();
    HttpURLConnection connection = null;
    DataOutputStream outputStream = null;
    BufferedReader bufferedReader = null;
    String payload =
        "{\"@type\":\"alluxio.job.persist.PersistConfig\",\"filePath\":\"" + path.getPath() + "\"}";
    try {
      URL url = new URL("http://" + address.getHostName() + ":" + address.getPort()
          + Constants.REST_API_PREFIX + "/" + JobMasterClientRestServiceHandler.SERVICE_PREFIX + "/"
          + JobMasterClientRestServiceHandler.RUN_JOB);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Content-Length", Integer.toString(payload.length()));
      connection.setRequestProperty("Content-Language", "en-US");
      connection.setUseCaches(false);
      connection.setDoOutput(true);
      outputStream = new DataOutputStream(connection.getOutputStream());
      outputStream.writeChars(payload);
      outputStream.close();
      InputStream is = connection.getInputStream();
      bufferedReader = new BufferedReader(new InputStreamReader(is));
      StringBuilder response = new StringBuilder();
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        response.append(line);
        response.append('\r');
      }
      bufferedReader.close();
      LOG.info("scheduled async persist of file {}", path);
      LOG.debug("response: {}", response.toString());
    } catch (Exception e) {
      LOG.warn("failed to schedule async persistence", e);
    } finally {
      try {
        if (outputStream != null) {
          outputStream.close();
        }
      } catch (IOException e) {
        LOG.warn("failed to closed output stream");
      }
      try {
        if (bufferedReader != null) {
          bufferedReader.close();
        }
      } catch (IOException e) {
        LOG.warn("failed to closed buffered reader");
      }
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @Override
  public List<PersistFile> pollFilesToPersist(long workerId) {
    // the files are persisted by job service, so this method is not used
    return Lists.newArrayList();
  }
}
