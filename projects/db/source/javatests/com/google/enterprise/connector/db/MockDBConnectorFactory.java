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
import com.google.enterprise.connector.spi.RepositoryException;
import com.google.enterprise.connector.util.diffing.DiffingConnector;

import junit.framework.Assert;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class MockDBConnectorFactory implements ConnectorFactory {
  public MockDBConnectorFactory() {
  }

  /**
   * Creates a database connector.
   *
   * @param config map of configuration values.
   */
  /* TODO(jlacey): Extract the Spring instantiation code in CM. */
  @Override
  public Connector makeConnector(Map<String, String> config)
      throws RepositoryException {
    // TODO(jlacey): The placeholder values are in the EPPC bean in
    // connectorDefaults.xml, but we're not loading that, and doing so
    // would unravel a ball of string: using setLocation instead of
    // setProperties (since the EPPC bean already has properties),
    // which in turn requires the ByteArrayResource machinery in
    // InstanceInfo or writing the properties to a file.
    Properties props = new Properties();
    for (String configKey : DBConnectorType.CONFIG_KEYS) {
      props.put(configKey, "");
    }
    // Escape MyBatis syntax that looks like a Spring placeholder.
    // See https://jira.springsource.org/browse/SPR-4953
    props.put("dollarSign", "$");
    props.put("docIds", "${dollarSign}{docIds}");
    props.putAll(config);

    Resource prototype = new ClassPathResource("config/connectorInstance.xml",
        MockDBConnectorFactory.class);
    Resource defaults = new ClassPathResource("config/connectorDefaults.xml",
        MockDBConnectorFactory.class);

    DefaultListableBeanFactory factory = new DefaultListableBeanFactory();
    XmlBeanDefinitionReader beanReader = new XmlBeanDefinitionReader(factory);
    try {
      beanReader.loadBeanDefinitions(prototype);
      beanReader.loadBeanDefinitions(defaults);
    } catch (BeansException e) {
      throw new RepositoryException(e);
    }

    PropertyPlaceholderConfigurer cfg = new PropertyPlaceholderConfigurer();
    cfg.setProperties(props);
    cfg.postProcessBeanFactory(factory);

    String[] beanList = factory.getBeanNamesForType(DiffingConnector.class);
    Assert.assertEquals(Arrays.asList(beanList).toString(), 1, beanList.length);
    return (Connector) factory.getBean(beanList[0]);
  }
}
