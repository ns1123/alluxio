/*
 * Copyright (c) 2016 Alluxio, Inc. All rights reserved.
 *
 * This software and all information contained herein is confidential and proprietary to Alluxio,
 * and is protected by copyright and other applicable laws in the United States and other
 * jurisdictions. You may not use, modify, reproduce, distribute, or disclose this software without
 * the express written permission of Alluxio.
 */

package alluxio.job.fs;

import alluxio.client.ReadType;
import alluxio.client.WriteType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * The interface layer to communicate with different file systems, such as Alluxio or HDFS.
 */
public interface AbstractFS {
  /**
   * Closes the connection to the file system.
   *
   * @throws IOException if the close fails
   */
  public abstract void close() throws IOException;

  /**
   * Creates a file. Use the default block size and write type.
   *
   * @param path the file's full path
   * @return the output stream of the created file
   * @throws IOException if the file creation fails
   */
  public abstract OutputStream create(String path) throws IOException;

  /**
   * Creates a file with a specified block size. Use the default write type.
   *
   * @param path the file's full path
   * @param blockSize the block size of the file
   * @return the output stream of the created file
   * @throws IOException if the file creation fails
   */
  public abstract OutputStream create(String path, long blockSize) throws IOException;

  /**
   * Creates a file with specified block size and write type. Do not create parent directories.
   *
   * @param path the file's full path
   * @param blockSize the block size of the file
   * @param writeType the write type of the file
   * @return the output stream of the created file
   * @throws IOException if the file creation fails
   */
  public abstract OutputStream create(String path, long blockSize, WriteType writeType)
      throws IOException;

  /**
   * Creates a file with specified block size and write type.
   *
   * @param path the file's full path
   * @param blockSize the block size of the file
   * @param writeType the write type of the file
   * @param recursive whether to recursively create the parent directories
   * @return the output stream of the created file
   * @throws IOException if the file creation fails
   */
  public abstract OutputStream create(String path, long blockSize, WriteType writeType,
      boolean recursive) throws IOException;

  /**
   * Creates a file with sepcified replication factor, for HDFS only.
   *
   * @param path the file's full path
   * @param replication the replication factor for HDFS file
   * @return the output stream of the created file
   * @throws IOException if the file creation fails
   */
  public abstract OutputStream create(String path, short replication) throws IOException;

  /**
   * Creates a directory.
   *
   * @param path the dir's path
   * @param writeType the Alluxio write type
   * @throws IOException if the dir creation fails
   */
  public abstract void createDirectory(String path, WriteType writeType) throws IOException;

  /**
   * Creates an empty file recursively.
   *
   * @param path the file's full path
   * @param writeType the Alluxio write type
   * @throws IOException if the file creation fails
   */
  public abstract void createEmptyFile(String path, WriteType writeType) throws IOException;

  /**
   * List path's status (corresponds to ls in unix).
   *
   * @param path the file or dir path
   * @throws IOException if the operation fails
   */
  public abstract void listStatusAndIgnore(String path) throws IOException;

  /**
   * Randomly read some data from the file for n times.
   * @param path the file path to read
   * @param fileSize the fileSize
   * @param bytesToRead the number of bytes to read
   * @param n the number of random reads to perform
   * @throws IOException if the operation fails
   */
  public abstract void randomReads(String path, long fileSize, int bytesToRead, int n)
      throws IOException;

  /**
   * Deletes the file. If recursive is true and the path is a directory, it deletes all the files
   * under the path.
   *
   * @param path the file's full path
   * @param recursive If true, deletes recursively
   * @return ture if success, false otherwise
   * @throws IOException if the deletion fails
   */
  public abstract boolean delete(String path, boolean recursive) throws IOException;

  /**
   * Checks whether the file exists or not.
   *
   * @param path the file's full path
   * @return true if the file exists, false otherwise
   * @throws IOException if getting status of the file fails
   */
  public abstract boolean exists(String path) throws IOException;

  /**
   * Gets the length of the file, in bytes.
   *
   * @param path the file's full path
   * @return the length of the file in bytes
   * @throws IOException if getting status of the file fails
   */
  public abstract long getLength(String path) throws IOException;

  /**
   * Checks whether the path is a directory.
   *
   * @param path the file's full path
   * @return true if it's a directory, false otherwise
   * @throws IOException if getting status of the file fails
   */
  public abstract boolean isDirectory(String path) throws IOException;

  /**
   * Checks whether the path is a file.
   *
   * @param path the file's full path
   * @return true if it's a file, false otherwise
   * @throws IOException if getting status of the file fails
   */
  public abstract boolean isFile(String path) throws IOException;

  /**
   * Lists the files under the path. If the path is a file, returns the full path of the file. If
   * the path is a directory, returns the full paths of all the files under the path. Otherwise
   * returns null.
   *
   * @param path the file's full path
   * @return the list contains the full paths of the listed files
   * @throws IOException if getting status of the file fails
   */
  public abstract List<String> listFullPath(String path) throws IOException;

  /**
   * Creates the directory named by the path. If the folder already exists, the method returns
   * false.
   *
   * @param path the file's full path
   * @param createParent If true, the method creates any necessary but nonexistent parent
   *                     directories. Otherwise, the method does not create nonexistent parent
   * @return true if success, false otherwise
   * @throws IOException if fails the create the parent directories
   */
  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  /**
   * Opens a file and returns its input stream. Use the default read type if needed.
   *
   * @param path the file's full path
   * @return the input stream of the opened file
   * @throws IOException if the file open fails
   */
  public abstract InputStream open(String path) throws IOException;

  /**
   * Opens a file and returns its input stream, with the specified read type.
   *
   * @param path the file's full path
   * @param readType the read type of the file
   * @return the input stream of the opened file
   * @throws IOException if the file open fails
   */
  public abstract InputStream open(String path, ReadType readType) throws IOException;

  /**
   * Renames the file.
   *
   * @param src the source full path
   * @param dst the destination full path
   * @return true if success, false otherwise
   * @throws IOException if the rename fails
   */
  public abstract boolean rename(String src, String dst) throws IOException;
}
