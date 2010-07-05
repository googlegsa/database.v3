//Copyright 2010 Google Inc.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.google.db.util;

import org.hibernate.Hibernate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * This class represent Employee_info table in database. This class is mapped
 * with the Employee_Info table in Database.
 * 
 * @author suresh_ghuge
 */
public class Employee {

	private String lastName;
	private String firstName;
	private Integer empId;
	private Integer deptId;
	private byte[] report;

	public byte[] getReport() {
		return report;
	}

	public void setReport(byte[] report) {
		this.report = report;
	}

	/** Don't invoke this. Used by Hibernate only. */
	public void setReportBlob(Blob reportBlob) {
		this.report = this.toByteArray(reportBlob);
	}

	/** Don't invoke this. Used by Hibernate only. */
	public Blob getReportBlob() {
		return Hibernate.createBlob(this.report);

	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public Integer getEmpId() {
		return empId;
	}

	public void setEmpId(Integer empId) {
		this.empId = empId;
	}

	public Integer getDeptId() {
		return deptId;
	}

	public void setDeptId(Integer deptId) {
		this.deptId = deptId;
	}

	/**
	 * This method converts BLOB object into array of bytes
	 * 
	 * @param fromBlob BLOB object
	 * @return
	 */
	private byte[] toByteArray(Blob fromBlob) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			return toByteArrayImpl(fromBlob, baos);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (baos != null) {
				try {
					baos.close();
				} catch (IOException ex) {
				}
			}
		}
	}

	/**
	 * This method writes content of BLOB object into ByteArrayOutputStream and
	 * return the array of bytes.
	 * 
	 * @param fromBlob input BLOB object
	 * @param baos BLOB object will be written in ByteArrayOutputStream
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 */
	private byte[] toByteArrayImpl(Blob fromBlob, ByteArrayOutputStream baos)
			throws SQLException, IOException {
		// initial
		byte[] buf = new byte[4000];
		InputStream is = fromBlob.getBinaryStream();
		try {
			for (;;) {
				int dataSize = is.read(buf);

				if (dataSize == -1)
					break;
				baos.write(buf, 0, dataSize);
			}
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException ex) {
				}
			}
		}
		return baos.toByteArray();
	}
}
