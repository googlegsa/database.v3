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

import org.hibernate.Session;
import org.hibernate.Transaction;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class provide main method to run this utility. User need to supply
 * firstName lastName deptID emplNo and path of file to be uploaded as command
 * line argument. e.g Ramesh Rane 4 8 "C:\Documents and
 * Settings\abc\Desktop\abc.doc".
 * 
 * @author suresh_ghuge
 */
public class DBUtilMain {
	/**
	 * main method.
	 * 
	 * @param args
	 * @throws IOException throws IOException if input file is not found or
	 *             corrupted
	 */
	public static void main(String[] args) throws IOException {
		Employee emp1 = new Employee();

		// set employee first name
		emp1.setFirstName(args[0]);
		// set employee last name
		emp1.setLastName(args[1]);
		// parse and set department id
		emp1.setDeptId(Integer.parseInt(args[2]));
		// parse and set employee id
		emp1.setEmpId(Integer.parseInt(args[3]));

		DBUtilMain main = new DBUtilMain();
		// create File object from given file path.
		File reportFile = new File(args[4]);
		// create InputStream for given file
		InputStream is = new FileInputStream(reportFile);
		int length = (int) reportFile.length();
		// convert InputStream into array of byte
		byte[] report = getBytes(length, is);
		emp1.setReport(report);
		// save employee object
		main.saveEmployee(emp1);
	}

	/**
	 * This method will persist an employee object in underlying database.
	 * 
	 * @param emp1 Employee object
	 */
	private void saveEmployee(Employee emp1) {
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction trx = session.beginTransaction();
		session.save(emp1);
		trx.commit();
	}

	/**
	 * @param length is length of file
	 * @param inStream input stream of input file
	 * @return array of byte representing a input file
	 */
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
