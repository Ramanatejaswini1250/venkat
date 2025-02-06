import java.sql.{Connection, DriverManager, Statement, ResultSet, ResultSetMetaData}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{SparkSession, Row}
import scala.collection.mutable.ListBuffer

val spark = SparkSession.builder().getOrCreate()

// JDBC connection details
val url = "jdbc:mysql://<hostname>:<port>/<database>"
val user = "<username>"
val password = "<password>"

var conn: Connection = null
var stmt_rss: Statement = null
var resultSet: ResultSet = null

try {
  // Establish JDBC connection
  conn = DriverManager.getConnection(url, user, password)
  stmt_rss = conn.createStatement()

  // Execute query
  resultSet = stmt_rss.executeQuery("SELECT * FROM your_table")
  
  // Process the ResultSet and transform data
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
      if (i == 4) formattedEventTimestamp  // Replace 4th column (event_timestamp)
      else if (i == 6) formattedAlertDueDate // Replace 6th column (alert_due_date)
      else resultSet.getObject(i) // Other columns remain unchanged
    }

    // Add the row to ListBuffer
    rows += Row(row: _*)
  }

  // Convert ListBuffer to RDD and save as CSV
  val rdd = spark.sparkContext.parallelize(rows.toList)
  val outputPath = "hdfs://namenode:8020/user/hadoop/output/formatted_data.csv"
  rdd.saveAsTextFile(outputPath)

  println("Data has been saved to HDFS successfully.")

} catch {
  case e: Exception =>
    println(s"An error occurred: ${e.getMessage}")
} finally {
  if (resultSet != null) resultSet.close() // Close resultSet when done
  if (stmt_rss != null) stmt_rss.close()   // Close statement
  if (conn != null) conn.close()           // Close connection
}
