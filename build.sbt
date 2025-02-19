import org.apache.spark.sql.{Row, SparkSession}
import java.io.{BufferedReader, FileReader}
import javax.mail._
import javax.mail.internet._
import java.util.Properties
import scala.collection.mutable

object AlertCodeDataComparison {

  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("Alert Code Data Comparison with Email Notification")
      .master("local[*]") // Adjust as needed
      .getOrCreate()

    // Step 1: Load data from master_table1
    val masterTableDF = spark.sql("SELECT * FROM master_table1")

    // Collect the data as a map with alert_code as the key
    val masterDataMap = masterTableDF.collect().groupBy(row => row.getString(1))

    // Step 2: Read data from the CSV file
    val csvFilePath = "/path/to/master1.csv"
    val csvDataMap = readCsvData(csvFilePath)

    // Step 3: Compare data for each alert code
    val mismatchedRecords = mutable.ArrayBuffer[String]()

    // Compare each alert code in the master table with the CSV
    masterDataMap.foreach { case (alertCode, masterRows) =>
      val csvRows = csvDataMap.getOrElse(alertCode, Array.empty[Array[String]])

      // Compare row-by-row
      if (masterRows.length != csvRows.length) {
        mismatchedRecords += s"Alert Code: $alertCode - Row count mismatch. Master Count: ${masterRows.length}, CSV Count: ${csvRows.length}"
      } else {
        // Compare row contents
        for (i <- masterRows.indices) {
          val masterRow = masterRows(i)
          val csvRow = csvRows(i)

          if (masterRow.toSeq.map(_.toString) != csvRow) {
            mismatchedRecords += s"Alert Code: $alertCode - Mismatch found. Master Row: ${masterRow.mkString(",")}, CSV Row: ${csvRow.mkString(",")}"
          }
        }
      }
    }

    // Step 4: Send email if mismatches are found
    if (mismatchedRecords.nonEmpty) {
      val subject = "Alert Code Data Mismatch Detected"
      val body = "The following data mismatches were found:\n" + mismatchedRecords.mkString("\n")
      sendEmail("your_email@example.com", subject, body)
    } else {
      println("Data comparison successful. No mismatches found.")
    }
  }

  // Function to read data from the CSV and group it by alert code
  def readCsvData(csvFilePath: String): Map[String, Array[Array[String]]] = {
    val bufferedReader = new BufferedReader(new FileReader(csvFilePath))
    val csvData = mutable.Map[String, mutable.ArrayBuffer[Array[String]]]()

    var line = bufferedReader.readLine() // Read the header
    line = bufferedReader.readLine() // Start from the first data line

    while (line != null) {
      val columns = line.split(",").map(_.trim)
      val alertCode = columns(1) // Assuming the second column is alert_code
      csvData.getOrElseUpdate(alertCode, mutable.ArrayBuffer()) += columns
      line = bufferedReader.readLine()
    }

    bufferedReader.close()
    csvData.mapValues(_.toArray).toMap
  }

  // Function to send email
  def sendEmail(to: String, subject: String, body: String): Unit = {
    val from = "your_email@example.com"
    val host = "smtp.example.com"
    val properties = System.getProperties
    properties.setProperty("mail.smtp.host", host)

    val session = Session.getDefaultInstance(properties)
    try {
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(from))
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(to))
      message.setSubject(subject)
      message.setText(body)
      Transport.send(message)
      println("Email sent successfully.")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
