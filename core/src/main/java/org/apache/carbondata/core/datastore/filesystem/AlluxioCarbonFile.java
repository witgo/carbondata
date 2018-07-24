/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.filesystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;

import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AlluxioCarbonFile extends AbstractDFSCarbonFile {
  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AlluxioCarbonFile.class.getName());

  public AlluxioCarbonFile(String filePath) {
    super(filePath);
  }

  public AlluxioCarbonFile(Path path) {
    super(path);
  }

  public AlluxioCarbonFile(FileStatus fileStatus) {
    super(fileStatus);
  }

  /**
   * @param listStatus
   * @return
   */
  @Override
  protected CarbonFile[] getFiles(FileStatus[] listStatus) {
    if (listStatus == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] files = new CarbonFile[listStatus.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new AlluxioCarbonFile(listStatus[i]);
    }
    return files;
  }

  @Override
  public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
    CarbonFile[] files = listFiles();
    if (files != null && files.length >= 1) {
      List<CarbonFile> fileList = new ArrayList<CarbonFile>(files.length);
      for (int i = 0; i < files.length; i++) {
        if (fileFilter.accept(files[i])) {
          fileList.add(files[i]);
        }
      }
      if (fileList.size() >= 1) {
        return fileList.toArray(new CarbonFile[fileList.size()]);
      } else {
        return new CarbonFile[0];
      }
    }
    return files;
  }

  @Override
  public CarbonFile getParentFile() {
    Path parent = fileStatus.getPath().getParent();
    return null == parent ? null : new AlluxioCarbonFile(parent);
  }

  @Override
  public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType)
      throws IOException {
    return getDataOutputStream(path, fileType, CarbonCommonConstants.BYTEBUFFER_SIZE, true);
  }

  @Override
  public boolean renameForce(String changetoName) {
    FileSystem fs;
    try {
      deleteFile(changetoName, FileFactory.getFileType(changetoName));
      fs = fileStatus.getPath().getFileSystem(hadoopConf);
      return fs.rename(fileStatus.getPath(), new Path(changetoName));
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }

  @Override
  public DataOutputStream getDataOutputStream(
      String path,
      FileFactory.FileType fileType,
      int bufferSize,
      boolean append) throws IOException {
    Path pt = new Path(path);
    FileSystem fileSystem = pt.getFileSystem(FileFactory.getConfiguration());
    FSDataOutputStream stream;
    if (append) {
      // append to a file only if file already exists else file not found
      // exception will be thrown by hdfs
      if (CarbonUtil.isFileExists(path)) {
        LOGGER.warn("Appending data to an existing alluxio file may cause consistency issues");
        DataInputStream dataInputStream = fileSystem.open(pt);
        int count = dataInputStream.available();
        // create buffer
        byte[] byteStreamBuffer = new byte[count];
        int bytesRead = dataInputStream.read(byteStreamBuffer);
        fileSystem.delete(pt,true);
        stream = fileSystem.create(pt, true, bufferSize);
        stream.write(byteStreamBuffer, 0, bytesRead);
      } else {
        stream = fileSystem.create(pt, true, bufferSize);
      }
    } else {
      stream = fileSystem.create(pt, true, bufferSize);
    }
    return stream;
  }
}
