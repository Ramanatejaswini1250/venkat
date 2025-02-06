import java.sql.{Connection, DriverManager, Statement, ResultSet, ResultSetMetaData}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{SparkSession, Row}
import java.io.{BufferedWriter, FileWriter}

val spark = SparkSession.builder().getOrCreate()

// JDBC connection details
val url = "jdbc:mysql://<hostname>:<port>/<database>"
val user = "<username>"
val password = "<password>"

// Read DataFrame from the database
val df = spark.read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", "your_table")
  .option("user", user)
  .option("password", password)
  .load()

// Write data to CSV without using parallelize
df.foreachPartition { partition =>
  // JDBC connection for each partition
  var conn: Connection = null
  var stmt_rss: Statement = null
  var resultSet: ResultSet = null

  try {
    // Establish JDBC connection for each partition
    conn = DriverManager.getConnection(url, user, password)
    stmt_rss = conn.createStatement()

    // Execute the query to fetch data from the table
    resultSet = stmt_rss.executeQuery("SELECT * FROM your_table")

    // Prepare to write to CSV file
    val outputPath = "hdfs://namenode:8020/user/hadoop/output/formatted_data.csv"
    val writer = new BufferedWriter(new FileWriter(outputPath, true))  // Append mode

    // Iterate through ResultSet and transform data
    while (resultSet.next()) {
      // Extract columns (assuming event_timestamp is 4th and alert_due_date is 6th)
      val eventTimestamp = resultSet.getString(4)  // 4th column
      val alertDueDate = resultSet.getString(6)    // 6th column

      // Format the event_timestamp and alert_due_date
      val formattedEventTimestamp = if (eventTimestamp != null) {
        LocalDateTime.parse(eventTimestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
          .format(DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a"))
      } else {
        "Unknown"
      }

      val formattedAlertDueDate = if (alertDueDate != null) {
        LocalDateTime.parse(alertDueDate, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
          .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))
      } else {
        "Unknown"
      }

      // Create the transformed row in CSV format with the updated columns
      val transformedRow = s"${resultSet.getString(1)},${resultSet.getString(2)},${resultSet.getString(3)},$formattedEventTimestamp,${resultSet.getInt(5)},$formattedAlertDueDate"
      
      // Write the transformed row to the CSV file
      writer.write(transformedRow)
      writer.newLine()
    }

    // Close the writer
    writer.close()

  } catch {
    case e: Exception =>
      println(s"An error occurred: ${e.getMessage}")
  } finally {
    if (resultSet != null) resultSet.close() // Close resultSet when done
    if (stmt_rss != null) stmt_rss.close()   // Close statement
    if (conn != null) conn.close()           // Close connection
  }
}

println("Data has been saved to CSV successfully.")
