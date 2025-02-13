import scala.collection.mutable.ListBuffer

// Create a ListBuffer to store the rows
val formattedMasterTable1DF = new ListBuffer[String]()

// Step 1: Extract headers dynamically using metadata
val metaData = masterTable1.getMetaData
val columnCount = metaData.getColumnCount

// Construct the header row
val headers = (1 to columnCount).map(i => metaData.getColumnName(i)).mkString(",")
formattedMasterTable1DF += headers  // Add the headers as the first row

// Step 2: Iterate through the result set and add rows
while (masterTable1.next()) {
  // Extract column values using masterTable1.getString(index)
  val EVENT_TIMESTAMP = masterTable1.getString(4)
  val ALERT_DUE_DATE = masterTable1.getString(6)

  // Format dates if necessary
  val formattedEventTimestamp = if (EVENT_TIMESTAMP != null) {
    LocalDateTime
      .parse(EVENT_TIMESTAMP, DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a"))
      .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))
  } else {
    "Unknown"
  }

  val formattedAlertDueDate = if (ALERT_DUE_DATE != null) {
    LocalDateTime
      .parse(ALERT_DUE_DATE, DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a"))
      .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))
  } else {
    "Unknown"
  }

  // Manually construct the row using getString(index) for each column
  val row = s"${masterTable1.getString(1)},${masterTable1.getString(2)},${masterTable1.getString(3)},$formattedEventTimestamp,${masterTable1.getInt(5)},$formattedAlertDueDate"

  // Step 3: Add the row to the ListBuffer
  formattedMasterTable1DF += row
}

// Step 4: Output or write the ListBuffer content to a CSV file
formattedMasterTable1DF.foreach(println)  // Print for debugging
