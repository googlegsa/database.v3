package com.google.enterprise.connector.db;

import java.util.List;
/**
 * Interface implemented by different validation classes in @link ValidateUtil.  
 */

public interface ConfigValidation {
	boolean validate();

	String getMessage();

	List<String> getProblemFields();
}
