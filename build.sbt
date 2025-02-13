import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Example function to format timestamps (if needed)
def formatTimestamp(timestamp: String, pattern: String): String = {
  if (timestamp != null) {
    try {
      LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        .format(DateTimeFormatter.ofPattern(pattern))
    } catch {
      case _: Exception => "Invalid Date"
    }
  } else {
    "Unknown"
  }
}

val masterTable1 = stmt_rss.executeQuery(Master1ValidationQuery)
val formattedMasterTable1DF = new ListBuffer[Array[String]]()

// Dynamically read data and format as required
while (masterTable1.next()) {
  val eventTimestamp = masterTable1.getString(4) // Assuming 4th column is EVENT_TIMESTAMP
  val alertDueDate = masterTable1.getString(6)   // Assuming 6th column is ALERT_DUE_DATE

  // Format the timestamps
  val formattedEventTimestamp = formatTimestamp(eventTimestamp, "dd-MM-yyyy hh:mm a")
  val formattedAlertDueDate = formatTimestamp(alertDueDate, "dd/MM/yyyy HH:mm")

  // Add formatted data as an array of strings
  formattedMasterTable1DF += Array(
    masterTable1.getString(1),  // Example: alert_id
    masterTable1.getString(2),  // Example: alert_code
    masterTable1.getString(3),  // Example: business_line
    formattedEventTimestamp,
    masterTable1.getString(5),  // Example: priority
    formattedAlertDueDate
  )
}

// Write to CSV
val csvFormatPath1 = "/path/to/your/output.csv"
val outputStream = fs.create(new Path(csvFormatPath1))
val masterWriter1 = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))

try {
  // Write headers dynamically
  val headers = Array("alert_id", "alert_code", "business_line", "event_timestamp", "priority", "alert_due_date")
  masterWriter1.write(headers.mkString(","))
  masterWriter1.newLine()

  // Write each row to the CSV
  formattedMasterTable1DF.foreach { row =>
    masterWriter1.write(row.mkString(","))
    masterWriter1.newLine()
  }

  println(s"CSV Files were written to...$csvFormatPath1")
} catch {
  case ex: Exception =>
    ex.printStackTrace()
} finally {
  masterWriter1.flush()
  masterWriter1.close()
  outputStream.close()
}

// Optional: Read back the CSV (if needed)
val inputStream = fs.open(new Path(csvFormatPath1))
val master1CSvRows = new ListBuffer[String]()
val master1_br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))

println(s"Reading from csvFormatPath1: $csvFormatPath1")

// Read and print the header
val header = master1_br.readLine()
println(s"Header: $header")

// Read and print each row
var line = master1_br.readLine()
while (line != null) {
  master1CSvRows += line
  line = master1_br.readLine()
}

println("Rows:")
master1CSvRows.foreach(println)

// Close the input stream
master1_br.close()
inputStream.close()
