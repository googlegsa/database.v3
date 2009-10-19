# Copyright 2009 Google Inc. All Rights Reserved.
# Author: meghna@google.com (Meghna Dhar)
#
# Description:
#   Database connector.


java_library(name = "dbconnector",
             srcs = glob(["*.java"], exclude = ["DbConnectorMain.java"]),
             deps = [ "//third_party/java/google_enterprise_connector_manager:connector-spi",
                      "//third_party/java/ibatis:ibatis",
                      "//third_party/java/joda_time",
                    ],
             resources = glob(["config/*"]),
             )

# The purpose of this executable is to create a runfiles directory
# (blaze-bin/java/com/google/enterprise/connector/db/dbconnector_main.runfiles)
# that gathers all the dependencies of the dbconnector so they can be
# easily copied to the connector manager's library.
java_binary(name = "dbconnector_main",
            srcs = [ "DbConnectorMain.java"],
            deps = [ ":dbconnector"],
            )

# This creates a .jar file in the format required by the CM. The contents are
# the same as the :dbconnector jar except that the config files have been
# copied to the to the top level.
# TODO(meghna): Add support for adding more language specific properties files.
genrule(name = "DbConnectorJar",
        srcs = [ "//java/com/google/enterprise/connector/db:dbconnector",
                 ],
        outs = [ "DbConnector.jar" ],
        cmd = "cp $(location :dbconnector) $@ && " +
              "$(JAR) -xf $(location :dbconnector) com/google/enterprise/connector/db/config/connectorInstance.xml && " +
              "$(JAR) -xf $(location :dbconnector) com/google/enterprise/connector/db/config/connectorType.xml && " +
              "$(JAR) -uf $@ -C com/google/enterprise/connector/db config/connectorInstance.xml && " +
              "$(JAR) -uf $@ -C com/google/enterprise/connector/db config/connectorType.xml && " +
              "$(JAR) -xf $(location :dbconnector) com/google/enterprise/connector/db/config/DbConnectorResources.properties &&" +
              "$(JAR) -uf $@ -C com/google/enterprise/connector/db config/DbConnectorResources.properties &&" +
              "rm -r com/"
        )
# These are jars should be filtered out.
FILTERED_JARS = (
                 "libdbconnector.jar " +
                 "connector-spi.jar " +
                 "dbconnector_main.jar "
                )

# This generates a WAR file containing the Connector Manager and the
# DbConnector. This WAR is needed for testing but can also be used
# to install a DB Connector with a supporting Connector Manager in
# tomcat. It includes the mysql jar for testing purposes.
CONNECTOR_MANAGER_WAR = ("//third_party/java/google_enterprise_connector_manager:connector-manager-war")
genrule (name = "db-connector-war",
         srcs = [ CONNECTOR_MANAGER_WAR,
                  ":dbconnector_main_deploy.jar",
                  ":DbConnectorJar",
                  "//third_party/java/jdbc/mysql:mysql-5.1.6.jar",
                ],
         outs = ["connector-manager.war"],
         cmd = # Clean up old work.
              "rm -fr $(@D)/war && " +
              "rm -fr $(@D)/temp && " +

              # Create working area.
              "mkdir $(@D)/war && " +

              # Create temp working area.
              "mkdir $(@D)/temp && " +

              # Unpack the CM WAR file.
              "CM_WAR_FILE=$$(pwd)/$(location " + CONNECTOR_MANAGER_WAR + ") && " +
              "pushd $(@D)/war && " +
              "$(JAR) xf $$CM_WAR_FILE && " +
              "popd && " +


              # Copy over all JAR files using backup suffix in case of name collision.
              "find $$(dirname $(location :dbconnector_main_deploy.jar))/dbconnector_main.runfiles/google3 -name *.jar" +
              " -exec cp --backup=t {} $(@D)/temp/ \; && " +


              # Filter files not needed in the final connector manager war.
              "for FILE in " + FILTERED_JARS + ";do" +
              "  rm -f $(@D)/temp/$${FILE};" +
              "done && " +

              "cp -r $(@D)/temp/* $(@D)/war/WEB-INF/lib/ &&" +

              # Copy DbConnector.jar to lib directory.
              "cp $(location :DbConnectorJar) $(@D)/war/WEB-INF/lib && " +

              # Copy mysql jar file required for ABCDE test.
              "cp $(location //third_party/java/jdbc/mysql:mysql-5.1.6.jar) $(@D)/war/WEB-INF/lib && " +

              # Create combined WAR file.
              "$(JAR) cfm $@ $(@D)/war/META-INF/MANIFEST.MF -C $(@D)/war ."
         )