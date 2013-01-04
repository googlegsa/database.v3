-- Creating Tables – Double hyphens are comment lines

DROP TABLE IF EXISTS  user_doc_map ;
CREATE TABLE  user_doc_map(
  id int NOT NULL,
  ReportId int NOT NULL ,
  username  varchar(45) NOT NULL,
  PRIMARY KEY (id)
);
