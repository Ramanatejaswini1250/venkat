import org.apache.spark.sql.SparkSession
import java.io.{BufferedReader, FileReader}
import scala.collection.mutable

object AlertCodeComparison {

  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("Alert Code Comparison")
      .master("local[*]") // Adjust as needed for your environment
      .getOrCreate()

    // Step 1: Load the master table from the database
    val masterTableDF = spark.sql("SELECT * FROM master_table1")

    // Step 2: Count occurrences of each alert_code in the master table
    val masterAlertCodeCounts = masterTableDF.groupBy("alert_code")
      .count()
      .collect() // Collect to driver for comparison
      .map(row => (row.getString(0), row.getLong(1).toInt))
      .toMap

    // Step 3: Read and count alert codes from the CSV file
    val csvFilePath = "/path/to/master1.csv"
    val csvAlertCodeCounts = readCsvAlertCodeCounts(csvFilePath)

    // Step 4: Compare counts
    val mismatchedAlertCodes = mutable.ArrayBuffer[String]()
    masterAlertCodeCounts.foreach { case (alertCode, masterCount) =>
      val csvCount = csvAlertCodeCounts.getOrElse(alertCode, 0)
      if (masterCount != csvCount) {
        mismatchedAlertCodes += s"Alert Code: $alertCode, Master Count: $masterCount, CSV Count: $csvCount"
      }
    }

    // Step 5: Output results
    if (mismatchedAlertCodes.isEmpty) {
      println("All alert counts match successfully.")
    } else {
      println("Mismatched Alert Codes Found:")
      mismatchedAlertCodes.foreach(println)
    }

    spark.stop()
  }

  // Function to read a CSV file and count occurrences of alert codes
  def readCsvAlertCodeCounts(filePath: String): mutable.Map[String, Int] = {
    val alertCodeCountMap = mutable.Map[String, Int]()
    val bufferedReader = new BufferedReader(new FileReader(filePath))
    bufferedReader.readLine() // Skip header

    var line: String = bufferedReader.readLine()
    while (line != null) {
      val columns = line.split(",") // Assuming CSV is comma-separated
      if (columns.length > 0) {
        val alertCode = columns(0).trim // Assuming alert code is in the first column
        val count = alertCodeCountMap.getOrElse(alertCode, 0)
        alertCodeCountMap.update(alertCode, count + 1)
      }
      line = bufferedReader.readLine()
    }
    bufferedReader.close()
    alertCodeCountMap
  }
}
