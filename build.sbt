import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import java.sql.{ResultSet, ResultSetMetaData}

// Assuming you have an existing Spark session
val spark = SparkSession.builder().getOrCreate()

// Step 1: Execute the query and get the ResultSet
val resultSet: ResultSet = stmt_rss.executeQuery("SELECT * FROM master")

// Step 2: Retrieve the metadata of the ResultSet to understand the columns
val metadata: ResultSetMetaData = resultSet.getMetaData
val columnCount = metadata.getColumnCount

// Step 3: Read the data from ResultSet row by row
val rows = new scala.collection.mutable.ListBuffer[Row]()
while (resultSet.next()) {
    // Collect data for each row
    val row = (1 to columnCount).map(i => resultSet.getObject(i)).toArray
    rows += Row.fromSeq(row)
}

// Step 4: Manually Apply Date Formatting and Other Transformations
val formattedRows = rows.map { row =>
  val eventTimestamp = row(2).toString  // Assuming event_timestamp is in the 3rd column
  val alertDueDate = row(3).toString   // Assuming alert_due_date is in the 4th column

  // Manually apply transformations (e.g., date formatting)
  val formattedEventTimestamp = java.time.LocalDateTime.parse(eventTimestamp, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    .format(java.time.format.DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a"))
  val formattedAlertDueDate = java.time.LocalDateTime.parse(alertDueDate, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    .format(java.time.format.DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))

  // Rebuild the row with transformed values
  Row.fromSeq(row.updated(2, formattedEventTimestamp).updated(3, formattedAlertDueDate))  // Replace transformed date columns
}

// Step 5: Count the Number of Records
val recordCount = formattedRows.size

// Step 6: Show Transformed Rows (Sample Display)
formattedRows.take(5).foreach(println)  // Show the first 5 rows to verify the transformation

// Step 7: Print the Total Record Count
println(s"Total Record Count: $recordCount")
