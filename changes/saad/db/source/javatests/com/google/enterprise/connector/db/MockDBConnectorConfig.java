package com.google.enterprise.connector.db;

import java.util.Map;
import java.util.Properties;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;



public class MockDBConnectorConfig {

	  private final String connectorInstanceXmlFile;

	  /**
	   * @param connectorInstanceXmlFile path to a "connectorInstance.xml" Spring
	   *        configuration file.
	   */
	  public MockDBConnectorConfig(String connectorInstanceXmlFile) {
	    this.connectorInstanceXmlFile = connectorInstanceXmlFile;
	  }

	  /**
	   * Creates a database connector.
	   *
	   * @param config map of configuration values.
	   */
	  /*@Override*/
	  public DBConnectorConfig makeConnector(Map<String, String> config)  {
	    Properties props = new Properties();
	    props.putAll(config);

	    Resource res = new ClassPathResource(connectorInstanceXmlFile,
	          MockDBConnectorFactory.class);
	    XmlBeanFactory factory = new XmlBeanFactory(res);
	    PropertyPlaceholderConfigurer cfg = new PropertyPlaceholderConfigurer();
	    cfg.setProperties(props);
	    cfg.postProcessBeanFactory(factory);
	    DBConnectorConfig connector = (DBConnectorConfig) factory.getBean("db-connector-config");
	    return connector;
	  }
}
