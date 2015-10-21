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

import static com.google.common.base.Charsets.UTF_8;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.connector.spi.AuthorizationManager;
import com.google.enterprise.connector.spi.AuthorizationResponse;
import com.google.enterprise.connector.spi.Connector;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.Session;
import com.google.enterprise.connector.spi.SimpleAuthenticationIdentity;
import com.google.enterprise.connector.util.Base64;

import java.util.Collection;

public class DBConnectorAuthorizationManagerTest extends DBTestBase {
  private AuthorizationManager authZmanager;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    /*
     * Create table that map username and permitted docIds.
     */
    runDBScript(CREATE_USER_DOC_MAP_TABLE);
    /*
     * Load test data.
     */
    runDBScript(LOAD_USER_DOC_MAP_TEST_DATA);

    // Spring-instantiate the connector to test the ${docIds} escaping.
    MockDBConnectorFactory connectorFactory = new MockDBConnectorFactory();
    Connector connector = connectorFactory.makeConnector(configMap);
    Session session = connector.login();
    authZmanager = session.getAuthorizationManager();
  }

  private void testAuthorizeDocids(ImmutableMap<String, Boolean> expected)
      throws RepositoryException {
    testAuthorizeDocids(expected.keySet(), expected);
  }

  private void testAuthorizeDocids(Collection<String> docIds,
      ImmutableMap<String, Boolean> expected) throws RepositoryException {
    SimpleAuthenticationIdentity authNIdentity =
        new SimpleAuthenticationIdentity("user1");

    Collection<AuthorizationResponse> authZResponse =
        authZmanager.authorizeDocids(docIds, authNIdentity);
    assertNotNull(authZResponse);
    assertEquals(authZResponse.toString(),
        expected.size(), authZResponse.size());

    for (AuthorizationResponse response : authZResponse) {
      String docId = response.getDocid();
      assertEquals(docId, expected.get(docId),
          Boolean.valueOf(response.isValid()));
    }
  }

  public void testAuthorizeDocids_basic() throws RepositoryException {
    testAuthorizeDocids(ImmutableMap.<String, Boolean>of(
        "l/1", true, "l/2", false, "l/3", true, "l/4", false));
  }

  /**
   * Docids without type tags are assumed to be Base64-encoded. Here
   * the decoding fails, so there are no docids to put in the query.
   */
  public void testAuthorizeDocids_untyped() throws RepositoryException {
    testAuthorizeDocids(ImmutableMap.<String, Boolean>of(
        "1", false, "2", false, "3", false, "4", false));
  }

  /**
   * The fix for untyped docids is small and partial. If only some
   * docids are invalid, they will not be returned by authorizeDocids,
   * and they will be assumed to be denied downstream.
   */
  public void testAuthorizeDocids_mixed() throws RepositoryException {
    testAuthorizeDocids(ImmutableList.of("1", "l/2", "l/3", "4"),
        ImmutableMap.<String, Boolean>of("l/2", false, "l/3", true));
  }

  private String base64(String input) {
    return Base64.encodeWebSafe(input.getBytes(UTF_8), false);
  }

  /** Base64-encoded docids are legacy but acceptable. */
  public void testAuthorizeDocids_base64() throws RepositoryException {
    testAuthorizeDocids(ImmutableMap.<String, Boolean>of(
        base64("1"), true, base64("2"), false,
        base64("3"), true, base64("4"), false));
  }

  /**
   * Some invalid docids can successfully be decoded into random garbage,
   * so they are returned by authorizeDocids.
   */
  public void testAuthorizeDocids_nonBase64() throws RepositoryException {
    testAuthorizeDocids(ImmutableMap.<String, Boolean>of(
        "hello-_world", false));
  }

  @Override
  protected void tearDown() throws Exception {
    // drop database table under test
    runDBScript(DROP_USER_DOC_MAP_TABLE);
  }
}
