import java.io.{BufferedWriter, OutputStreamWriter, FileOutputStream}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Assume you already have a valid JDBC ResultSet called masterTable1
// For example: val masterTable1: ResultSet = stmt_rss.executeQuery(Master1ValidationQuery)

// Create a ListBuffer to hold the CSV rows (including header)
val formattedMasterTable1DF = new ListBuffer[String]()

// -------------------------
// 1. Extract Headers Dynamically
// -------------------------
val metaData: ResultSetMetaData = masterTable1.getMetaData
val columnCount = metaData.getColumnCount
// Using getColumnLabel or getColumnName (whichever suits your needs)
val headers = (1 to columnCount).map(i => metaData.getColumnLabel(i)).mkString(",")
// Add the header as the first record in the ListBuffer
formattedMasterTable1DF += headers

// -------------------------
// 2. Process Each Row from the ResultSet
// -------------------------
while (masterTable1.next()) {
  // Example: format columns 4 and 6 (adjust indexes as needed)
  val EVENT_TIMESTAMP = masterTable1.getString(4)
  val ALERT_DUE_DATE  = masterTable1.getString(6)
  
  val formattedEventTimestamp = if (EVENT_TIMESTAMP != null) {
    try {
      LocalDateTime.parse(EVENT_TIMESTAMP, DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a"))
                .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))
    } catch {
      case _: Exception => "Unknown"
    }
  } else "Unknown"
  
  val formattedAlertDueDate = if (ALERT_DUE_DATE != null) {
    try {
      LocalDateTime.parse(ALERT_DUE_DATE, DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a"))
                .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))
    } catch {
      case _: Exception => "Unknown"
    }
  } else "Unknown"
  
  // Manually construct the CSV row.
  // Adjust the column indexes and types (e.g., getString, getInt) as needed.
  // This example assumes 6 columns where columns 4 and 6 are formatted.
  val rowString = s"${masterTable1.getString(1)}," +
                  s"${masterTable1.getString(2)}," +
                  s"${masterTable1.getString(3)}," +
                  s"$formattedEventTimestamp," +
                  s"${masterTable1.getString(5)}," +
                  s"$formattedAlertDueDate"
  
  // Add this row to the ListBuffer
  formattedMasterTable1DF += rowString
}

// (Optional) Debug print the entire ListBuffer content
println("Final CSV content:")
formattedMasterTable1DF.foreach(println)

// -------------------------
// 3. Write the ListBuffer to a CSV File Using BufferedWriter
// -------------------------
val csvLocalPath = "/path/to/local/output.csv"  // Change this to your desired file path
val fileOutputStream = new FileOutputStream(csvLocalPath)
val bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))

try {
  // Write header and rows:
  // Writing the header (first element) explicitly ensures it doesn't get skipped
  if (formattedMasterTable1DF.nonEmpty) {
    bufferedWriter.write(formattedMasterTable1DF.head)
    bufferedWriter.newLine()
    // Write the remaining rows
    formattedMasterTable1DF.tail.foreach { row =>
      bufferedWriter.write(row)
      bufferedWriter.newLine()
    }
  }
  println(s"CSV file written successfully to $csvLocalPath")
} catch {
  case ex: Exception =>
    ex.printStackTrace()
} finally {
  bufferedWriter.flush()
  bufferedWriter.close()
  fileOutputStream.close()
}
