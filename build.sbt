import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.{ResultSet, ResultSetMetaData}

// Assuming you have an existing Spark session
val spark = SparkSession.builder().getOrCreate()

// Step 1: Execute the query
val resultSet: ResultSet = stmt_rss.executeQuery("SELECT * FROM master")

// Step 2: Convert ResultSet to List of Rows
val metadata: ResultSetMetaData = resultSet.getMetaData
val columnCount = metadata.getColumnCount

val rows = new scala.collection.mutable.ListBuffer[Row]()
while (resultSet.next()) {
    val row = (1 to columnCount).map(i => resultSet.getObject(i)).toArray
    rows += Row.fromSeq(row)
}

// Step 3: Create Schema Dynamically
val schema = StructType((1 to columnCount).map { i =>
    StructField(metadata.getColumnName(i), StringType, nullable = true) // Change type if necessary
})

// Step 4: Convert List[Row] to Spark DataFrame
val resultSetDF = spark.createDataFrame(spark.sparkContext.parallelize(rows.toList), schema)

// Step 5: Format Date Columns
val formattedMasterTable1DF = resultSetDF
  .withColumn("event_timestamp", date_format(to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"), "dd-MM-yyyy hh:mm:ss a"))
  .withColumn("alert_due_date", date_format(to_timestamp(col("alert_due_date"), "yyyy-MM-dd HH:mm:ss"), "dd/MM/yyyy HH:mm"))

// Step 6: Count the Number of Records
val recordCount = formattedMasterTable1DF.count()

// Step 7: Show the Transformed DataFrame
formattedMasterTable1DF.show()

// Step 8: Print the Count
println(s"Total Record Count: $recordCount")
