import org.apache.spark.sql.Row

val rows = rowmasters.map { row =>
  // Assuming row is a Seq[Any] with exactly 6 elements.
  if (row.length != 6) {
    throw new RuntimeException(s"Expected 6 elements, but got ${row.length}: $row")
  }
  Row(
    row(0).asInstanceOf[String],
    row(1).asInstanceOf[String],
    row(2).asInstanceOf[String],
    row(3).asInstanceOf[String],
    row(4).asInstanceOf[Int],      // or row(4).toString.toInt if it's a string
    row(5).asInstanceOf[String]
  )
}



import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

// Create an empty ArrayBuffer to hold each row's data
val rowmasters = ArrayBuffer[ArrayBuffer[Any]]()

// Assume stmt is your JDBC Statement object and you execute your query
val resultSet = stmt.executeQuery("YOUR SQL QUERY")

while (resultSet.next()) {
  // Extract each column value from the ResultSet.
  // Adjust the column names and types as needed.
  val alertId = resultSet.getString("ALERT_ID")
  val alertCode = resultSet.getString("ALERT_CODE")
  val alertDesc = resultSet.getString("ALERT_DESC")
  val alertTimestamp = resultSet.getString("ALERT_TIMESTAMP")
  val priority = resultSet.getInt("PRIORITY")  // If this column is an int in the DB
  val alertDueDate = resultSet.getString("ALERT_DUE_DATE")
  
  // Append the row's data as an ArrayBuffer
  rowmasters += ArrayBuffer(alertId, alertCode, alertDesc, alertTimestamp, priority, alertDueDate)
}
import org.apache.spark.sql.Row

// Convert each element of rowmasters (an ArrayBuffer of values) into a Row.
val rows = rowmasters.map {
  case ArrayBuffer(alertId, alertCode, alertDesc, alertTimestamp, priority, alertDueDate) =>
    Row(alertId, alertCode, alertDesc, alertTimestamp, priority.toString.toInt, alertDueDate)
}

// Then parallelize it to create an RDD (assuming you're in a Spark context)
val rdd = spark.sparkContext.parallelize(rows)

// Define your schema appropriately
import org.apache.spark.sql.types._
val schema = StructType(Seq(
  StructField("ALERT_ID", StringType, true),
  StructField("ALERT_CODE", StringType, true),
  StructField("ALERT_DESC", StringType, true),
  StructField("ALERT_TIMESTAMP", StringType, true),
  StructField("PRIORITY", IntegerType, true),
  StructField("ALERT_DUE_DATE", StringType, true)
))

// Create the DataFrame
val df = spark.createDataFrame(rdd, schema)
df.show()
