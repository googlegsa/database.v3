package com.google.enterprise.connector.db;

import java.util.List;

public interface ConfigValidation {
	boolean validate();

	String getMessage();

	List<String> getProblemFields();
}
