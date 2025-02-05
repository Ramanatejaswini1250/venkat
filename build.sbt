import java.sql.{Connection, DriverManager, Statement, ResultSet, ResultSetMetaData}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{SparkSession, Row}
import scala.collection.mutable.ListBuffer

// Initialize Spark session
val spark = SparkSession.builder().getOrCreate()

// JDBC connection details (replace with your own)
val url = "jdbc:mysql://<hostname>:<port>/<database>"
val user = "<username>"
val password = "<password>"

// Establish JDBC connection
val conn = DriverManager.getConnection(url, user, password)
val stmt_rss = conn.createStatement()

// Step 1: Execute query and get ResultSet
val resultSet: ResultSet = stmt_rss.executeQuery("SELECT * FROM your_table")

// Step 2: Process ResultSet and transform data
val rows = new ListBuffer[Row]()
val metadata: ResultSetMetaData = resultSet.getMetaData
val columnCount = metadata.getColumnCount

// Iterate through the ResultSet
while (resultSet.next()) {
  // Extract columns (assuming event_timestamp is 4th and alert_due_date is 6th)
  val eventTimestamp = resultSet.getString(4)  // 4th column
  val alertDueDate = resultSet.getString(6)    // 6th column

  // Format the event_timestamp and alert_due_date
  val formattedEventTimestamp = LocalDateTime.parse(eventTimestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    .format(DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a"))
  val formattedAlertDueDate = LocalDateTime.parse(alertDueDate, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))

  // Create a new row with formatted data
  val row = (1 to columnCount).map { i =>
    if (i == 4) formattedEventTimestamp // Replace 4th column (event_timestamp)
    else if (i == 6) formattedAlertDueDate // Replace 6th column (alert_due_date)
    else resultSet.getObject(i) // Other columns remain unchanged
  }

  // Add the row to ListBuffer
  rows += Row(row: _*)
}

// Step 3: Save transformed data as CSV to HDFS

// Convert ListBuffer to RDD
val rdd = spark.sparkContext.parallelize(rows.toList)

// Write the RDD as CSV to HDFS (make sure to replace with your HDFS path)
val outputPath = "hdfs://namenode:8020/user/hadoop/output/formatted_data.csv"
rdd.saveAsTextFile(outputPath)

// Step 4: Validate by comparing row counts

// Get the count of records from the table and CSV
val tableRecordCount = rows.size
val csvDf = spark.read.option("header", "false").csv(outputPath)
val csvRecordCount = csvDf.count()

if (tableRecordCount == csvRecordCount) {
  println(s"Row count matches: $tableRecordCount records")
} else {
  println(s"Row count mismatch! Table count: $tableRecordCount, CSV count: $csvRecordCount")
}

// Optionally, compare specific columns (event_timestamp and alert_due_date)
val originalFormattedTimestamps = rows.map { row =>
  (row(0), row(1), row(2), row(3), row(4), row(5), row(6)) // Original format
}

val csvFormattedTimestamps = csvDf.collect().map { row =>
  (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6)) // CSV format
}

// You can compare originalFormattedTimestamps and csvFormattedTimestamps row by row if needed

// Close connection and statement
stmt_rss.close()
conn.close()
