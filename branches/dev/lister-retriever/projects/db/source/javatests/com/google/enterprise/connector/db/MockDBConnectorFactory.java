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

import com.google.enterprise.connector.spi.Connector;
import com.google.enterprise.connector.spi.ConnectorFactory;
import com.google.enterprise.connector.util.diffing.DiffingConnector;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.util.Map;
import java.util.Properties;

public class MockDBConnectorFactory implements ConnectorFactory {
  private final String connectorInstanceXmlFile;

  /**
   * @param connectorInstanceXmlFile path to a "connectorInstance.xml" Spring
   *        configuration file.
   */
  public MockDBConnectorFactory(String connectorInstanceXmlFile) {
    this.connectorInstanceXmlFile = connectorInstanceXmlFile;
  }

  /**
   * Creates a database connector.
   *
   * @param config map of configuration values.
   */
  /* @Override */
  public Connector makeConnector(Map<String, String> config) {
    Properties props = new Properties();
    props.putAll(config);

    Resource res = new ClassPathResource(connectorInstanceXmlFile,
        MockDBConnectorFactory.class);
    XmlBeanFactory factory = new XmlBeanFactory(res);
    PropertyPlaceholderConfigurer cfg = new PropertyPlaceholderConfigurer();
    cfg.setProperties(props);
    cfg.postProcessBeanFactory(factory);
    DiffingConnector connector = null;
    return connector;
  }
}
