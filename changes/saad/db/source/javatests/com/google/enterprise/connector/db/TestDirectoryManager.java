// Copyright 2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

import com.google.common.io.Files;
import junit.framework.TestCase;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class TestDirectoryManager {
  private final File tmpDir;
  TestDirectoryManager(TestCase testCase) throws IOException {
    String baseTmpDir = System.getProperty("java.io.tmpdir");
    File parent = new File(baseTmpDir).getAbsoluteFile();
    parent = new File(parent, testCase.getClass().getSimpleName());
    parent = new File(parent, "tmp");
    tmpDir = new File(parent, "d-" + UUID.randomUUID().toString());
    if (this.tmpDir.exists()) {
      Files.deleteRecursively(tmpDir);
    }
    if (!tmpDir.mkdirs()) {
      throw new IOException("can't create test dir: " + tmpDir);
    }
  }
  
  public String getTmpDir() throws IOException {
    return tmpDir.getCanonicalPath();
  }
  
  public String getTestDataDir() {
    String currentWorkDir = System.getProperty("user.dir");
    String testDataDir = currentWorkDir + "/testdata/";
    return testDataDir;
  }
  
  public String getSrcDir() {
    return System.getProperty("user.dir");
  }
}
