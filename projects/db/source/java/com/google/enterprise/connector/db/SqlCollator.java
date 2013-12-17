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

import com.google.common.base.Objects;

import java.text.Collator;
import java.text.CollationKey;
import java.util.Comparator;

/**
 * An implementation of {@code java.text.Collator} that uses a SQL query
 * to perform the comparison.
 */
public class SqlCollator extends Collator {
  private final DBClient dbClient;
  private String collationId;
  private String collationQuery;

  /**
   * Creates a new {@link SqlCollator} the uses a database connection
   * supplied by the {@link #dbClient} to compare text strings.
   *
   * @param dbClient the database client
   */
  public SqlCollator(DBClient dbClient) {
    this.dbClient = dbClient;
  }

  /**
   * Sets the {@link #collationId} which identifies which SQL collation rules
   * to use when comparing the strings. It <em>must</em> match the collation
   * rules used by the Traversal SQL Query ORDER BY clause. For instance,
   * if you are using SQL Server and you specified
   * {@code ORDER BY name COLLATE Latin1_General_CS_AI}, then
   * {@code collationId} would be {@code Latin1_General_CS_AI}.
   * Similarly, if you are using Oracle and your ORDER BY clause was
   * {@code ORDER BY NLSSORT(name, 'NLS_SORT = Latin1_AI')} then
   * {@code collationId} would be {@code Latin1_AI}.  If {@code collationId} is
   * not specified, then the default collation for the database will be used.
   * </p>
   * Specifying a {@code collationId} is only supported for Oracle and
   * databases that support standard SQL COLLATE syntax (including SQL Server).
   * If your database does not support standard SQL COLLATE syntax, you may
   * specify a custom collation query using {@link #setCollationQuery(String)}
   * instead.  You should only call one of {@link #setCollationQuery(String)}
   * or {@link #setCollationId(String)}. Setting the collation query takes
   * precedence over setting the collation identifier.
   *
   * @param collationId the collation rules identifier
   */
  public void setCollationId(String collationId) {
    this.collationId = collationId;
  }

  /** Returns the collation identifier, or null if one was not set. */
  public String getCollationId() {
    return collationId;
  }

  /**
   * Sets the custom {@code #collationQuery} to use when comparing two strings.
   * The ordering performed by this query <em>must</em> match the collation
   * rules used by the Traversal SQL Query ORDER BY clause.
   * </p>
   * The query string must include parameters for the source and target strings
   * to compare, identified as <code>${source}</code> and
   * <code>${target}</code>, respectively.
   * </p>
   * The query should return two or fewer rows of a single column of
   * strings.  If fewer than two rows are returned, the strings are assumed
   * to be equivalent according to the rules of collation.  If two rows are
   * returned, the lesser string should be returned in the first row.
   * </p>
   * A sample collation query that uses the default collation rules for the
   * database might look like:
   * <code>
     SELECT name
     FROM (
       SELECT '${source}' AS name
       UNION
       SELECT '${target}'
     ) AS temp
     ORDER BY name
     </code>
   * </p>
   * Specifying a {@code collationQuery} may be required if your database
   * does not support the standard SQL COLLATE syntax (other than Oracle,
   * which we special-case internally).
   * </p>
   * You should only call one of {@link #setCollationQuery(String)}
   * or {@link #setCollationId(String)}.  Setting the collation query takes
   * precedence over setting the collation identifier.
   *
   * @param collationQuery a custom SQL query that compares two strings.
   */
  public void setCollationQuery(String collationQuery) {
    this.collationQuery = collationQuery;
  }

  /** Returns the custom collation query, or null if one was not set. */
  public String getCollationQuery() {
    return collationQuery;
  }

  /**
   * Compares the source string to the target string using a SQL query to
   * perform the comparison and using the collation rules as specified by
   * the {@code collationQuery} or {@code collationId}.
   *
   * @param source the source string
   * @param target the target string
   * @return an integer less than, equal to, or greater than zero depending
   * on whether the source string is less than, equal to, or greater than
   * the target string.
   */
  @Override
  public int compare(String source, String target) {
    return dbClient.executeCollationQuery(source, target);
  }

  /** {@code getCollationKey} is not supported by this implementation. */
  @Override
  public CollationKey getCollationKey(String source) {
    throw new UnsupportedOperationException("getCollationKey is not supported");
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(collationId, collationQuery);
  }
}
