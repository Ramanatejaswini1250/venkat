try {
  // Get column names dynamically
  val headers = formattedMasterTable1DF.schema.fieldNames.mkString(",")
  master_writer1.write(headers)
  master_writer1.newLine()

  // Write the rows
  formattedMasterTable1DF.foreach { row =>
    // Convert each row to a CSV format string
    val rowAsString = row.mkString(",")
    master_writer1.write(rowAsString)
    master_writer1.newLine()
  }

  println(s"CSV Files were written to...$csvFormatPath1")
} catch {
  case ex: Exception =>
    ex
