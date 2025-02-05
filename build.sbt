import org.apache.spark.sql.Row
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val spark = SparkSession.builder().getOrCreate()

// Assuming `rows` is your ListBuffer of transformed rows
val rows: scala.collection.mutable.ListBuffer[Row] = // your transformed rows here

// Convert each Row into a CSV line (String)
val csvRows = rows.map { row =>
  // Create a CSV line as a String
  val eventTimestamp = row(3).toString  // 4th column (index 3)
  val alertDueDate = row(5).toString    // 6th column (index 5)

  // Apply transformations (e.g., date formatting)
  val formattedEventTimestamp = LocalDateTime.parse(eventTimestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    .format(DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a"))
  val formattedAlertDueDate = LocalDateTime.parse(alertDueDate, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))

  // Create the CSV string (make sure to join all columns with commas)
  s"${row(0)},${row(1)},${row(2)},$formattedEventTimestamp,${row(4)},$formattedAlertDueDate,${row(6)}"
}

// Convert ListBuffer to RDD
val rdd = spark.sparkContext.parallelize(csvRows.toList)

// Save RDD as a CSV file to HDFS
rdd.saveAsTextFile("hdfs://<namenode>/path/to/output/folder")
