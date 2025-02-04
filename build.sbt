import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.ResultSet

// Initialize Spark session
val spark = SparkSession.builder().appName("ResultSetToDataFrame").master("local[*]").getOrCreate()

// Define schema (ensure this matches the columns you're extracting)
val schema_master1 = StructType(Seq(
  StructField("alert_id", StringType, nullable = true),
  StructField("alert_code", StringType, nullable = true),
  StructField("business_line", StringType, nullable = true),
  StructField("event_timestamp", StringType, nullable = true)
))

// Get metadata to check column names
val metaData = masterTable1.getMetaData
val columnCount = metaData.getColumnCount
println(s"Column count: $columnCount")

// Print column names for debugging
for (i <- 1 to columnCount) {
  println(s"Column $i: ${metaData.getColumnName(i)}")
}

// Initialize an empty list to hold rows
var rows_master1 = List[Row]()

// Check if there are any records in the ResultSet
if (masterTable1.isBeforeFirst()) {
  masterTable1.beforeFirst()  // Reset to the first row

  // Iterate over all rows in the ResultSet
  while (masterTable1.next()) {
    // Dynamically extract values for each column in the row
    val rowValues = (1 to columnCount).map { i =>
      val columnName = metaData.getColumnName(i).toLowerCase
      val columnValue = masterTable1.getString(i) // Extract column value as String
      println(s"Extracted $columnName: $columnValue") // Debugging output to ensure correct extraction
      columnValue
    }

    // Debug: print the entire row of values
    println("Row Data: " + rowValues.mkString(", "))

    // Create a Row and add to the list
    rows_master1 = rows_master1 :+ Row.fromSeq(rowValues)
  }

  // Convert the List of Rows to a DataFrame
  if (rows_master1.nonEmpty) {
    val masterTable1DF = spark.createDataFrame(
      spark.sparkContext.parallelize(rows_master1),
      schema_master1  // Ensure this matches the columns in the ResultSet
    )

    // Show the DataFrame
    masterTable1DF.show()

    // Optional: Apply transformations like formatting the event_timestamp
    val formattedDF = masterTable1DF.withColumn(
      "formatted_event_timestamp",
      date_format(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    // Show the transformed DataFrame
    formattedDF.show()

  } else {
    println("No rows to display in DataFrame!")
  }

} else {
  println("No records found in the ResultSet!")
}
