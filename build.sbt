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

// Step 4: Validate by comparing data

// Read the CSV data back into DataFrame
val csvDf = spark.read.option("header", "false").csv(outputPath)

// Step 5: Compare original data with CSV data row by row

// Extracting the transformed data from the ListBuffer
val originalData = rows.map { row =>
  val formattedEventTimestamp = row(3).toString
  val formattedAlertDueDate = row(5).toString
  (row(0).toString, row(1).toString, row(2).toString, formattedEventTimestamp, row(4).toString, formattedAlertDueDate, row(6).toString)
}

// Extracting data from CSV
val csvData = csvDf.collect().map { row =>
  (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6))
}

// Step 6: Compare each row of the original data with the corresponding row from the CSV file
var dataConsistent = true
for (i <- originalData.indices) {
  if (originalData(i) != csvData(i)) {
    println(s"Mismatch found at row $i: Original: ${originalData(i)} != CSV: ${csvData(i)}")
    dataConsistent = false
  }
}

if (dataConsistent) {
  println("Data is consistent between table and CSV.")
} else {
  println("Data mismatch found between table and CSV.")
}

// Close connection and statement
stmt_rss.close()
conn.close()
