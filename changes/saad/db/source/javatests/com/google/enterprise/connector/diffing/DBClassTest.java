package com.google.enterprise.connector.diffing;

import com.google.enterprise.connector.spi.Document;
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.spi.SpiConstants;
import com.google.enterprise.connector.util.diffing.DocumentHandle;
import com.google.enterprise.connector.util.diffing.DocumentSnapshot;

import junit.framework.TestCase;

public class DBClassTest extends TestCase {

	JsonDocument jsonDocument;
	protected void setUp() throws Exception {
		
		JsonObjectUtil jsonObjectUtil=new JsonObjectUtil();
		jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "1");
		jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
		jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, "text/plain");
		jsonDocument=new JsonDocument(jsonObjectUtil.getJsonObject());
		
	}

	
	public void testFactoryFunction(){
		DBClass dbClass;
		dbClass=DBClass.factoryFunction.apply(jsonDocument);
		assertNotNull(dbClass);
	
	}
	
	public void testGetDocument() {
		
		DBClass dbClass;
		dbClass=new DBClass(jsonDocument);
		try {
			Document document=dbClass.getDocument();
			assertNotNull(document);
		} catch (RepositoryException e) {
			System.out.println("Repository Exception");
		}
		
	}

	public void testGetDocumentId() {
		DBClass dbClass;
		dbClass=new DBClass(jsonDocument);
		String expected="1";
		assertEquals(expected, dbClass.getDocumentId());		
	}

	public void testGetUpdate() {
		
		DocumentHandle documentHandle=new DBClass(jsonDocument); 
		DocumentSnapshot documentSnapshot=new DBClass(jsonDocument);
		try {
			DocumentHandle actual=documentSnapshot.getUpdate(null);
			// the diffing framework sends in a null to indicate that it hasn't seen
			// this snapshot before. So we return the corresponding Handle (in our
			// case,
			// the same object)
			assertEquals(documentHandle.getDocument(), actual.getDocument());
			
			// we just assume that if the serialized form is the same, then nothing
			// has changed.
			assertNull(documentSnapshot.getUpdate(documentSnapshot));
		
			// Something has changed, so return the corresponding handle
			JsonObjectUtil jsonObjectUtil=new JsonObjectUtil();
			jsonObjectUtil.setProperty(SpiConstants.PROPNAME_DOCID, "2");
			jsonObjectUtil.setProperty(SpiConstants.PROPNAME_ISPUBLIC, "false");
			jsonObjectUtil.setProperty(SpiConstants.PROPNAME_MIMETYPE, "text/plain");
			DocumentSnapshot newdocumentSnapshot=new DBClass(new JsonDocument(jsonObjectUtil.getJsonObject()));
			assertNotSame( documentHandle.getDocument(), documentSnapshot.getUpdate(newdocumentSnapshot));
		
		} catch (RepositoryException e) {
			System.out.println("Repository Exception");		}
	}

	public void testToString() {
		DBClass dbClass;
		String expected="{\"google:ispublic\":\"false\",\"google:docid\":\"1\",\"google:mimetype\":\"text/plain\"}";
		dbClass=new DBClass(jsonDocument);
		assertEquals(expected,dbClass.toString());
		
	}

}
