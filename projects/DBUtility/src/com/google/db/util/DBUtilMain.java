//Copyright 2009 Google Inc.
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

import org.hibernate.Session;
import org.hibernate.Transaction;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DBUtilMain {
	public static void main(String[] args) throws IOException {
		Employee emp1 = new Employee();

		emp1.setFirstName(args[0]);
		emp1.setLastName(args[1]);
		emp1.setDeptId(Integer.parseInt(args[2]));
		emp1.setEmpId(Integer.parseInt(args[3]));

		DBUtilMain main = new DBUtilMain();

		File reportFile = new File(args[4]);
		InputStream is = new FileInputStream(reportFile);
		int length = (int) reportFile.length();
		byte[] report = getBytes(length, is);
		emp1.setReport(report);

		main.saveEmployee(emp1);
	}

	private void saveEmployee(Employee emp1) {
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction trx = session.beginTransaction();
		session.save(emp1);
		trx.commit();
	}

	// Returns the contents of the file in a byte array.
	public static byte[] getBytesFromFile(File file) throws IOException {
		InputStream is = new FileInputStream(file);

		// Get the size of the file
		long length = file.length();

		// You cannot create an array using a long type.
		// It needs to be an int type.
		// Before converting to an int type, check
		// to ensure that file is not larger than Integer.MAX_VALUE.
		if (length > Integer.MAX_VALUE) {
			// File is too large
		}

		// Create the byte array to hold the data
		byte[] bytes = new byte[(int) length];

		// Read in the bytes
		int offset = 0;
		int numRead = 0;
		while (offset < bytes.length
				&& (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
			offset += numRead;
		}

		// Ensure all the bytes have been read in
		if (offset < bytes.length) {
			throw new IOException("Could not completely read file "
					+ file.getName());
		}

		// Close the input stream and return bytes
		is.close();
		return bytes;
	}

	private static byte[] getBytes(int length, InputStream inStream) {

		int bytesRead = 0;
		byte[] content = new byte[length];
		while (bytesRead < length) {
			int result;
			try {
				result = inStream.read(content, bytesRead, length - bytesRead);
				if (result == -1)
					break;
				bytesRead += result;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return content;
	}

}
