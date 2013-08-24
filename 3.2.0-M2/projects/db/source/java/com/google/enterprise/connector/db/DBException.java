// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.enterprise.connector.db;

/**
 * This is a package level class for generic database connector exceptions.
 */
public class DBException extends Exception {

  /**
   * @param message
   */
  public DBException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public DBException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public DBException(String message, Throwable cause) {
    super(message, cause);
  }
}
