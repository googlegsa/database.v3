// Copyright 2013 Google Inc.
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

import java.text.Collator;

public interface ValueOrdering {
  /**
   * Returns {@code true} if NULLs sort low in this database implementation;
   * or {@code false} if NULLs sort high.
   */
  boolean nullsAreSortedLow();

  /**
   * Returns a {@link Collator} implementation that mimics the sorting done by
   * the database {@code ORDER BY} clause specified in the Traversal SQL query
   * for text values (CHAR, VARCHAR, etc).
   */
  Collator getCollator();
}
