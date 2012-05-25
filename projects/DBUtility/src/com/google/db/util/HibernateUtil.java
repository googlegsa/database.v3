// Copyright 2009 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.db.util;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.io.File;

/**
 * This is an util class for getting Hibernate Session object. User need to
 * replace "REPLACE WITH ACTUAL PATH" with actual path to the config file of
 * hibernate before compiling and tunning this utility.
 *
 * @author suresh_ghuge
 */
public class HibernateUtil {

  private static final SessionFactory sessionFactory;

  static {
    try {
      sessionFactory = new Configuration().configure(new File(
          "REPLACE WITH ACTUAL PATH")).buildSessionFactory();
      /* example :
       * sessionFactory = new Configuration().configure(new File(
       * "E:\\workspace\\DBUtility\\src\\hibernate.mysql.cfg.xml"
       * )).buildSessionFactory();
       */

    } catch (Throwable ex) {
      System.err.println("Initial SessionFactory creation failed." + ex);
      throw new ExceptionInInitializerError(ex);
    }
  }

  public static SessionFactory getSessionFactory() {
    return sessionFactory;

  }
}
