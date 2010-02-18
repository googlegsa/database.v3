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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import junit.framework.TestCase;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class XmlUtilsTest extends TestCase {
	private Map<String, Object> rowMap;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		rowMap = TestUtils.getStandardDBRow();
	}

	@Override
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
			String rowXml = XmlUtils.getXMLRow("testdb_", rowMap, TestUtils.getStandardPrimaryKeys(), null);
			assertTrue(rowXml.contains(expected));
		} catch (DBException e) {
			fail(" Caught exception");
		}
	}

	public final void testGetXmlRowEmptyStylesheet() {
		String expected = "<html>\n<body>\n"
				+ "<title>Database Connector Result id=1 lastName=last_01 </title>\n"
				+ "<table border=\"1\">\n<tr bgcolor=\"#9acd32\">\n"
				+ "<th>id</th><th>lastName</th><th>email</th><th>firstName</th>\n"
				+ "</tr>\n<tr>\n<td>1</td><td>last_01</td><td>01@google.com</td>"
				+ "<td>first_01</td>\n</tr>\n</table>\n</body>\n</html>\n";
		try {
			String rowXml = XmlUtils.getXMLRow("testdb_", rowMap, TestUtils.getStandardPrimaryKeys(), "");
			assertEquals(expected, rowXml);
		} catch (DBException e) {
			fail(" Caught exception");
		}
	}

	public final void testGetXmlRowWithXslt() {
		String expected = "<html>\n"
				+ "<body>\n"
				+ "<title>Database Connector Result id=1 lastName=last_01 </title>\n"
				+ "<table border=\"1\">\n"
				+ "<tr bgcolor=\"#9acd32\">\n"
				+ "<th>First Name</th><th>Last Name</th><th>Email</th><th>Id</th>\n"
				+ "</tr>\n"
				+ "<tr>\n"
				+ "<td>first_01</td><td>last_01</td><td>01@google.com</td><td>1</td>\n"
				+ "</tr>\n" + "</table>\n" + "</body>\n" + "</html>";
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
		try {
			String rowXml = XmlUtils.getXMLRow("testdb_", rowMap, TestUtils.getStandardPrimaryKeys(), xslt);
			assertTrue(rowXml.contains(expected));
		} catch (DBException e) {
			fail(" Caught exception");
		}
	}

	public final void testGetStringFromDomDcoument() {
		String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
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

		try {
			Document doc = XmlUtils.getDomDocFromXslt(expectedXslt);
			assertEquals(expectedXslt, XmlUtils.getStringFromDomDocument(doc, null));
		} catch (TransformerException e) {
			fail("Could not get String from DOM document.\n" + e.toString());
		} catch (DBException e) {
			fail("Could not generate DOM document from base64 encoded XSLT string.\n"
					+ e.toString());
		}
	}
}
