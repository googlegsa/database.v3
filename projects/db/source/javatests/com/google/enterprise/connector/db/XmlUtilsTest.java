// Copyright 2011 Google Inc.
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

import com.google.enterprise.connector.db.DBException;
import com.google.enterprise.connector.db.XmlUtils;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import junit.framework.TestCase;

public class XmlUtilsTest extends TestCase {
	private Map<String, Object> rowMap;
	private static final Logger LOG = Logger.getLogger(XmlUtilsTest.class.getName());

    /* @Override */
	protected void setUp() throws Exception {
		super.setUp();
		rowMap = TestUtils.getStandardDBRow();
	}

    /* @Override */
	protected void tearDown() throws Exception {
		rowMap.clear();
		super.tearDown();
	}

    public final void testGetXMLRowNoXslt() {
		String expected = "<testdb_>"
				+ "<title>Database Connector Result id=1 lastName=last_01 "
				+ "</title><email>01@google.com</email>"
				+ "<firstName>first_01</firstName><id>1</id>"
				+ "<lastName>last_01</lastName>" + "</testdb_>";
		try {
			String rowXml = XmlUtils.getXMLRow("testdb_", rowMap, TestUtils.getStandardPrimaryKeys(), null, null, true);
			assertTrue(rowXml.contains(expected));
		} catch (DBException e) {
			fail(" Caught exception");
		}
	}

    public final void testGetXmlRowEmptyStylesheet() {
		String[] expectedPatterns = new String[] {
				"<title>Database Connector Result id=1 lastName=last_01 </title>",
				"<tr bgcolor=\"#9acd32\">", "<th>id</th>", "<th>lastName</th>",
				"<th>email</th>", "<th>firstName</th>", "<td>1</td>",
				"<td>last_01</td>", "<td>01@google.com</td>",
				"<td>first_01</td>" };
		try {
			String rowXml = XmlUtils.getXMLRow("testdb_", rowMap, TestUtils.getStandardPrimaryKeys(), "", null, true);
			assertCheckPatterns(rowXml, expectedPatterns);
		} catch (DBException e) {
			fail(" Caught exception");
		}
	}

    public final void testGetXmlRowWithXslt() {

        String xslt = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" "
				+ "standalone=\"no\"?><xsl:stylesheet xmlns:xsl="
				+ "\"http://www.w3.org/1999/XSL/Transform\" version=\"1.0\">\n\n"
				+ "<xsl:template match=\"/\">\n" + "  <html>\n" + "  <body>\n"
				+ "  <xsl:for-each select=\"testdb_\">\n"
				+ "  <title><xsl:value-of select=\"title\"/></title>\n"
				+ "  </xsl:for-each>\n" + "    <table border=\"1\">\n"
				+ "      <tr bgcolor=\"#9acd32\">\n"
				+ "        <th>First Name</th>\n"
				+ "        <th>Last Name</th>\n" + "        <th>Email</th>\n"
				+ "        <th>Id</th>\n" + "      </tr>\n"
				+ "      <xsl:for-each select=\"testdb_\">\n" + "      <tr>\n"
				+ "        <td><xsl:value-of select=\"firstName\"/></td>\n"
				+ "        <td><xsl:value-of select=\"lastName\"/></td>\n"
				+ "        <td><xsl:value-of select=\"email\"/></td>\n"
				+ "        <td><xsl:value-of select=\"id\"/></td>\n"
				+ "      </tr>\n" + "      </xsl:for-each>\n"
				+ "    </table>\n" + "  </body>\n" + "  </html>\n"
				+ "</xsl:template>\n" + "</xsl:stylesheet>";

        final String[] expectedPatterns = new String[] {
				"<title>Database Connector Result id=1 lastName=.*</title>",
				"<td>01@google.com</td>", "<td>first_01</td>", "<td>1</td>",
				"<td>last_01</td>" };
		try {
			String rowXml = XmlUtils.getXMLRow("testdb_", rowMap, TestUtils.getStandardPrimaryKeys(), xslt, null, true);
			assertCheckPatterns(rowXml, expectedPatterns);
		} catch (DBException e) {
			fail(" Caught exception");
		}
	}

    public final void testGetStringFromDomDcoument() {
		String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"
				+ "<testdb_>"
				+ "<title>Database Connector Result id=1 lastName=last_01 </title>"
				+ "<id>1</id><lastName>last_01</lastName>"
				+ "<email>01@google.com</email><firstName>first_01</firstName>"
				+ "</testdb_>";
		ByteArrayInputStream bais = new ByteArrayInputStream(
				expected.getBytes());
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
			org.w3c.dom.Document doc = builder.parse(bais);
			assertEquals(expected, XmlUtils.getStringFromDomDocument(doc, null));
		} catch (ParserConfigurationException e) {
			fail("Setup(creating a DOM document) for this test failed - not "
					+ "the actual test");
		} catch (SAXException e) {
			fail("Setup(creating a DOM document) for this test failed - not "
					+ "the actual test");
		} catch (IOException e) {
			fail("Setup(creating a DOM document) for this test failed - not "
					+ "the actual test");
		} catch (TransformerException e) {
			fail("Could not get String for the DOM Document");
		}
	}

    public final void testGetDocFromBase64Xslt() {
		String expectedXslt = "<?xml version=\"1.0\" encoding=\"UTF-8\""
				+ "?><xsl:stylesheet xmlns:xsl="
				+ "\"http://www.w3.org/1999/XSL/Transform\" version=\"1.0\">\n\n"
				+ "<xsl:template match=\"/\">\n" + "  <html>\n" + "  <body>\n"
				+ "  <xsl:for-each select=\"testdb_\">\n"
				+ "  <title><xsl:value-of select=\"title\"/></title>\n"
				+ "  </xsl:for-each>\n" + "    <table border=\"1\">\n"
				+ "      <tr bgcolor=\"#9acd32\">\n"
				+ "        <th>First Name</th>\n"
				+ "        <th>Last Name</th>\n" + "        <th>Email</th>\n"
				+ "        <th>Id</th>\n" + "      </tr>\n"
				+ "      <xsl:for-each select=\"testdb_\">\n" + "      <tr>\n"
				+ "        <td><xsl:value-of select=\"firstName\"/></td>\n"
				+ "        <td><xsl:value-of select=\"lastName\"/></td>\n"
				+ "        <td><xsl:value-of select=\"email\"/></td>\n"
				+ "        <td><xsl:value-of select=\"id\"/></td>\n"
				+ "      </tr>\n" + "      </xsl:for-each>\n"
				+ "    </table>\n" + "  </body>\n" + "  </html>\n"
				+ "</xsl:template>\n" + "</xsl:stylesheet>";

        String[] expectedPatterns = new String[] {
				"<xsl:for-each select=\"testdb_\">",
				"<title><xsl:value-of select=\"title\"/></title>",
				"<tr bgcolor=\"#9acd32\">", "<th>First Name</th>",
				"<th>Last Name</th>", "<th>Email</th>", "th>Id</th>",
				" <td><xsl:value-of select=\"firstName\"/></td>",
				"<td><xsl:value-of select=\"lastName\"/></td>",
				"<td><xsl:value-of select=\"email\"/></td>",
				"<td><xsl:value-of select=\"id\"/></td>" };
		try {
			Document doc = XmlUtils.getDomDocFromXslt(expectedXslt);
			String xmlDocString = XmlUtils.getStringFromDomDocument(doc, null);
			assertCheckPatterns(xmlDocString, expectedPatterns);
		} catch (TransformerException te) {
			fail("Could not get String from DOM document.\n" + te.toString());
		} catch (DBException dbe) {
			fail("Could not generate DOM document from base64 encoded XSLT string.\n"
					+ dbe.toString());
		}
	}

    /**
	 * Method search for pattern in document string. It gives an assertion error
	 * when accepted pattern does not found in document string.
	 * 
	 * @param docStringUnderTest String that represent actual document.
	 * @param expectedPatterns array of patterns that document String should
	 *            have
	 */
	private void assertCheckPatterns(final String docStringUnderTest,
			final String[] expectedPatterns) {

        Pattern pattern = null;
		Matcher match = null;

        for (String strPattern : expectedPatterns) {
			LOG.info("Checking for pattern  :   " + strPattern + "  ...");
			pattern = Pattern.compile(strPattern);
			match = pattern.matcher(docStringUnderTest);
			assertTrue(match.find());
		}
	}
}
