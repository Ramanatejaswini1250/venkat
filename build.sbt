import scala.collection.mutable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object AlertCodeComparison {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Alert Code Comparison")
      .master("local[*]") // Change this to the correct cluster mode
      .getOrCreate()

    // Sample MasterTable1 DataFrame (Replace with actual data loading)
    val masterTable1DF: DataFrame = spark.read
      .option("header", "true") // Ensures the first line is treated as header
      .csv("path/to/mastertable1.csv")

    // Group by alert code and calculate record counts
    val masterTable1AlertCodeCounts = masterTable1DF
      .groupBy("alert_code")
      .count()
      .collect()
      .map(row => (row.getString(0), row.getLong(1).toInt)) // Ensures proper type conversion to Int
      .toMap

    // Initialize mutable Map to hold alert code counts
    val alertCodeCountMap = mutable.Map[String, Int]()

    // Sample logic to update map
    masterTable1AlertCodeCounts.foreach { case (alertCode, count) =>
      val currentCount = alertCodeCountMap.getOrElse(alertCode, 0)
      alertCodeCountMap.update(alertCode, currentCount + count)
    }

    // Debug print for verification
    alertCodeCountMap.foreach { case (alertCode, count) =>
      println(s"Alert Code: $alertCode, Count: $count")
    }

    // Additional comparisons or validations can be performed here
    // For example: Compare masterTable1 counts with another table or CSV

