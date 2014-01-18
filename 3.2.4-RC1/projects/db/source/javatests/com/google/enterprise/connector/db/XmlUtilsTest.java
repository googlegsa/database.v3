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

import junit.framework.TestCase;

import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class XmlUtilsTest extends TestCase {
  private Map<String, Object> rowMap;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    rowMap = TestUtils.getStandardDBRow();
  }

  public void testGetXmlRowNoXslt() throws DBException {
    String expected = "<testdb_>"
        + "<title>Database Connector Result id=1 lastName=last_01</title>"
        + "<email>01@example.com</email>"
        + "<firstName>first_01</firstName><id>1</id>"
        + "<lastName>last_01</lastName>"
        + "</testdb_>";
    String rowXml = XmlUtils.getXMLRow("testdb_", rowMap,
        TestUtils.getStandardPrimaryKeys(), null, null, true);
    assertTrue(rowXml, rowXml.endsWith(expected));
  }

  public void testGetXmlRowEmptyStylesheet() throws DBException {
    String[] expectedPatterns = new String[] {
        "<title>Database Connector Result id=1 lastName=last_01</title>",
        "<tr bgcolor=\"#9acd32\">", "<th>id</th>", "<th>lastName</th>",
        "<th>email</th>", "<th>firstName</th>", "<td>1</td>",
        "<td>last_01</td>", "<td>01@example.com</td>", "<td>first_01</td>" };
    String rowXml = XmlUtils.getXMLRow("testdb_", rowMap,
        TestUtils.getStandardPrimaryKeys(), "", null, true);
    assertCheckPatterns(rowXml, expectedPatterns);
    assertTrue(rowXml, rowXml.indexOf("<html>") < rowXml.indexOf("<title>"));
    assertTrue(rowXml, rowXml.indexOf("</title>") < rowXml.indexOf("<body>"));
  }

  public void testGetXmlRowWithXslt() throws DBException {
    String xslt = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" "
        + "standalone=\"no\"?><xsl:stylesheet xmlns:xsl="
        + "\"http://www.w3.org/1999/XSL/Transform\" version=\"1.0\">\n\n"
        + "<xsl:template match=\"/\">\n" + "  <html>\n" + "  <body>\n"
        + "  <xsl:for-each select=\"testdb_\">\n"
        + "  <title><xsl:value-of select=\"title\"/></title>\n"
        + "  </xsl:for-each>\n" + "    <table border=\"1\">\n"
        + "      <tr bgcolor=\"#9acd32\">\n" + "        <th>First Name</th>\n"
        + "        <th>Last Name</th>\n" + "        <th>Email</th>\n"
        + "        <th>Id</th>\n" + "      </tr>\n"
        + "      <xsl:for-each select=\"testdb_\">\n" + "      <tr>\n"
        + "        <td><xsl:value-of select=\"firstName\"/></td>\n"
        + "        <td><xsl:value-of select=\"lastName\"/></td>\n"
        + "        <td><xsl:value-of select=\"email\"/></td>\n"
        + "        <td><xsl:value-of select=\"id\"/></td>\n" + "      </tr>\n"
        + "      </xsl:for-each>\n" + "    </table>\n" + "  </body>\n"
        + "  </html>\n" + "</xsl:template>\n" + "</xsl:stylesheet>";

    final String[] expectedPatterns = new String[] {
        "<title>Database Connector Result id=1 lastName=.*</title>",
        "<td>01@example.com</td>", "<td>first_01</td>", "<td>1</td>",
        "<td>last_01</td>" };
    String rowXml = XmlUtils.getXMLRow("testdb_", rowMap,
        TestUtils.getStandardPrimaryKeys(), xslt, null, true);
    assertCheckPatterns(rowXml, expectedPatterns);
  }

  public void testGetStringFromDomDcoument() throws Exception {
    String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"
        + "<testdb_>"
        + "<title>Database Connector Result id=1 lastName=last_01</title>"
        + "<id>1</id><lastName>last_01</lastName>"
        + "<email>01@example.com</email><firstName>first_01</firstName>"
        + "</testdb_>";
    ByteArrayInputStream bais = new ByteArrayInputStream(expected.getBytes());
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(bais);
    assertEquals(expected, XmlUtils.getStringFromDomDocument(doc, null));
  }

  public void testGetDocFromBase64Xslt() throws Exception {
    String expectedXslt = "<?xml version=\"1.0\" encoding=\"UTF-8\""
        + "?><xsl:stylesheet xmlns:xsl="
        + "\"http://www.w3.org/1999/XSL/Transform\" version=\"1.0\">\n\n"
        + "<xsl:template match=\"/\">\n" + "  <html>\n" + "  <body>\n"
        + "  <xsl:for-each select=\"testdb_\">\n"
        + "  <title><xsl:value-of select=\"title\"/></title>\n"
        + "  </xsl:for-each>\n" + "    <table border=\"1\">\n"
        + "      <tr bgcolor=\"#9acd32\">\n" + "        <th>First Name</th>\n"
        + "        <th>Last Name</th>\n" + "        <th>Email</th>\n"
        + "        <th>Id</th>\n" + "      </tr>\n"
        + "      <xsl:for-each select=\"testdb_\">\n" + "      <tr>\n"
        + "        <td><xsl:value-of select=\"firstName\"/></td>\n"
        + "        <td><xsl:value-of select=\"lastName\"/></td>\n"
        + "        <td><xsl:value-of select=\"email\"/></td>\n"
        + "        <td><xsl:value-of select=\"id\"/></td>\n" + "      </tr>\n"
        + "      </xsl:for-each>\n" + "    </table>\n" + "  </body>\n"
        + "  </html>\n" + "</xsl:template>\n" + "</xsl:stylesheet>";

    String[] expectedPatterns = new String[] {
        "<xsl:for-each select=\"testdb_\">",
        "<title><xsl:value-of select=\"title\"/></title>",
        "<tr bgcolor=\"#9acd32\">", "<th>First Name</th>",
        "<th>Last Name</th>", "<th>Email</th>", "th>Id</th>",
        " <td><xsl:value-of select=\"firstName\"/></td>",
        "<td><xsl:value-of select=\"lastName\"/></td>",
        "<td><xsl:value-of select=\"email\"/></td>",
        "<td><xsl:value-of select=\"id\"/></td>" };

    Document doc = XmlUtils.getDomDocFromXslt(expectedXslt);
    String xmlDocString = XmlUtils.getStringFromDomDocument(doc, null);
    assertCheckPatterns(xmlDocString, expectedPatterns);
  }

  /**
   * Method search for pattern in document string. It gives an assertion error
   * when accepted pattern does not found in document string.
   *
   * @param docStringUnderTest String that represent actual document.
   * @param expectedPatterns array of patterns that document String should have
   */
  private void assertCheckPatterns(final String docStringUnderTest,
      final String[] expectedPatterns) {
    for (String strPattern : expectedPatterns) {
      Pattern pattern = Pattern.compile(strPattern);
      Matcher match = pattern.matcher(docStringUnderTest);
      assertTrue("Did not find [" + strPattern + "] in string ["
          + docStringUnderTest + "]", match.find());
    }
  }
}
