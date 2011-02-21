package com.google.enterprise.connector.diffing;
import com.google.enterprise.connector.spi.SpiConstants;
import junit.framework.TestCase;
public class JsonDocumentTest extends TestCase {

	
	private JsonObjectUtil jsonObjectUtil;
	protected void setUp() throws Exception {
	 
		jsonObjectUtil=new JsonObjectUtil();
		jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "1");
		jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
		jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, "text/plain");
				
	}
	
	public void testJsonDocument() {
	
     JsonDocument jsonDocument=new JsonDocument(jsonObjectUtil.getJsonObject());
	assertNotNull(jsonDocument);
	}

	public void testGetDocumentId() {
		
		JsonDocument jsonDocument=new JsonDocument(jsonObjectUtil.getJsonObject());
		assertEquals("1", jsonDocument.getDocumentId());
	}

	public void testToJson() {
			
		String expected="{\"google:ispublic\":\"false\",\"google:docid\":\"1\",\"google:mimetype\":\"text/plain\"}";
		JsonDocument jsonDocument=new JsonDocument(jsonObjectUtil.getJsonObject());
		assertEquals(expected,jsonDocument.toJson());
		}

}
