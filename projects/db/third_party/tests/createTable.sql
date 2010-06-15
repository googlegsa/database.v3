-- Creating Tables – Double hyphens are comment lines
DROP TABLE IF EXISTS  TestEmpTable ;
CREATE TABLE TestEmpTable (
  id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
  fname VARCHAR(45),
  lname VARCHAR(45),
  dept INTEGER UNSIGNED,
  PRIMARY KEY (id)
);
