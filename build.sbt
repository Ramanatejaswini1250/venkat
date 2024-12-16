name := "EmailNotificationApp"

version := "0.1"

scalaVersion := "2.12.17"  // Adjust based on your Scala version

// Spark dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0",  // Change to the version you're using
                        "org.apache.spark" %% "spark-sql" % "3.4.0"

// JDBC driver (example for PostgreSQL, change as needed for your DB)
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.24" 

// Logging library (Optional, if you want better logging)
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.0-alpha1"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.0-alpha1"
