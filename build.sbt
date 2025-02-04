val schema_master1 = StructType(Seq(
  StructField("alert_id", StringType, nullable = true),
  StructField("alert_code", StringType, nullable = true),
  StructField("business_line", StringType, nullable = true),
  StructField("event_timestamp", StringType, nullable = true),
  StructField("priority", IntegerType, nullable = true),
  StructField("alert_due_date", StringType, nullable = true)
))

val rows_master1 = ArrayBuffer[Row]()

// Check and process the first row (or iterate through all rows if multiple)
if (masterTable1.next()) {
  do {
    val alertId = Option(masterTable1.getString("alert_id")).getOrElse(null)
    val alertCode = Option(masterTable1.getString("alert_code")).getOrElse(null)
    val businessLine = Option(masterTable1.getString("business_line")).getOrElse(null)
    val eventTimestamp = Option(masterTable1.getString("event_timestamp")).getOrElse(null)
    val alertDueDate = Option(masterTable1.getString("alert_due_date")).getOrElse(null)

    val priority = Option(masterTable1.getObject("priority")) match {
      case Some(value: Int) => value
      case _ => null
    }

    // Add the row to the buffer
    rows_master1 += Row(alertId, alertCode, businessLine, eventTimestamp, priority, alertDueDate)

  } while (masterTable1.next())  // Continue iterating over all rows
}

// Debugging: Ensure rows are correct before creating DataFrame
rows_master1.foreach(row => println(s"Row Data: $row"))

// Handle empty rows scenario and create DataFrame
val masterTable1DF = if (rows_master1.isEmpty) {
  println("Warning: No data found. Creating an empty DataFrame.")
  spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema_master1)  // Empty DataFrame
} else {
  spark.createDataFrame(rows_master1.toSeq, schema_master1)  // Convert to immutable Seq
}

masterTable1DF.show()
