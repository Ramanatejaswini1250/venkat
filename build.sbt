// build.sbt
name := "EmailNotificationApp"

version := "1.0"

scalaVersion := "2.12.18" // Ensure compatibility with Spark

// Spark and Hadoop dependencies
val sparkVersion = "3.5.0" // Adjust based on your Spark version
val hadoopVersion = "3.3.5" // Adjust based on your Hadoop version

libraryDependencies ++= Seq(
  // Spark core and SQL dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
libraryDependencies += "com.teradata" % "terajdbc4" % "version"
libraryDependencies += "com.teradata" % "tdgssconfig" % "version"

  
  // Logging (SLF4J and Log4j for better log management)
  "org.slf4j" % "slf4j-api" % "2.0.9",
  "org.slf4j" % "slf4j-log4j12" % "2.0.9",
  
  // Database (Teradata JDBC driver for database connection)
  "com.teradata.jdbc" % "terajdbc4" % "17.20.00.08", // Replace with your Teradata JDBC driver version
  "com.teradata.jdbc" % "tdgssconfig" % "17.20.00.08", // Teradata security configuration library

  // Configuration library for managing environment variables
  "com.typesafe" % "config" % "1.4.2"
)

// Compile options
scalacOptions ++= Seq(
  "-deprecation", // Warn about deprecated APIs
  "-feature",     // Warn about features
  "-unchecked",   // Enable additional warnings where generated code depends on assumptions
  "-Xfatal-warnings", // Fail the compilation on any warnings
  "-encoding", "utf8" // Specify UTF-8 character encoding
)

// SBT Assembly settings
enablePlugins(AssemblyPlugin)

// Specify the main class for running the project
mainClass in assembly := Some("EmailNotificationApp")

// Merge strategy for handling duplicate files during assembly
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Resolving dependency conflicts
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.15.2"

// Repository for dependencies
resolvers ++= Seq(
  "Apache Spark" at "https://repo1.maven.org/maven2/",
  "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/",
  "Teradata Maven Repository" at "https://artifactory.teradata.com/artifactory/maven-public/" // Teradata repository
)
