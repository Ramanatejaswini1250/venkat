import java.io.{BufferedReader, FileReader}
import java.util.Properties
import javax.mail.{Message, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}
import scala.collection.mutable
import org.apache.spark.sql.{SparkSession, DataFrame}

object AlertCodeValidator {

  def main(args: Array[String]): Unit = {
    val csvFilePath = "/path/to/master1.csv"
    val masterTablePath = "/path/to/masterTable1" // Replace with actual master table path or JDBC connection details

    // Step 1: Read CSV file and count alert codes
    val alertCodeCountMap = readCsvAlertCodeCounts(csvFilePath)

    // Step 2: Read master table (Assuming Spark DataFrame here)
    val spark = SparkSession.builder().appName("Alert Code Validator").getOrCreate()
    val masterTableDF = spark.read.option("header", "true").csv(masterTablePath) // Replace with actual table read method

    // Step 3: Group master table by alert code and count records
    val masterTableAlertCodeCounts = masterTableDF.groupBy("alert_code").count().collect().map(row =>
      row.getString(0) -> row.getLong(1).toInt
    ).toMap

    // Step 4: Compare CSV counts with master table counts
    val mismatchedAlertCodes = mutable.ArrayBuffer[String]()

    alertCodeCountMap.foreach { case (alertCode, csvCount) =>
      val masterCount = masterTableAlertCodeCounts.getOrElse(alertCode, 0)
      if (csvCount != masterCount) {
        mismatchedAlertCodes += s"Alert Code: $alertCode, CSV Count: $csvCount, Master Count: $masterCount"
      }
    }

    // Step 5: Send email or print success
    if (mismatchedAlertCodes.isEmpty) {
      println("All alert counts match successfully.")
    } else {
      println("Mismatched Alert Codes Found:")
      mismatchedAlertCodes.foreach(println)
      sendEmail(mismatchedAlertCodes)
    }

    // Stop Spark session
    spark.stop()
  }

  // Method to read CSV file using BufferedReader and count alert codes
  def readCsvAlertCodeCounts(filePath: String): mutable.Map[String, Int] = {
    val alertCodeCountMap = mutable.Map[String, Int]()
    val bufferedReader = new BufferedReader(new FileReader(filePath))
    bufferedReader.readLine() // Skip header

    var line: String = bufferedReader.readLine()
    while (line != null) {
      val columns = line.split(",") // Assuming CSV is comma-separated
      if (columns.length > 0) {
        val alertCode = columns(0).trim
        val count = alertCodeCountMap.getOrElse(alertCode, 0)
        alertCodeCountMap.update(alertCode, count + 1)
      }
      line = bufferedReader.readLine()
    }
    bufferedReader.close()
    alertCodeCountMap
  }

  // Method to send an email notification with mismatched alert codes
  def sendEmail(mismatchedAlertCodes: Seq[String]): Unit = {
    val smtpHost = "smtp.example.com" // Replace with actual SMTP host
    val fromEmail = "alerts@example.com"
    val toEmail = "recipient@example.com"

    val properties = new Properties()
    properties.put("mail.smtp.host", smtpHost)
    val session = Session.getDefaultInstance(properties)

    val message = new MimeMessage(session)
    message.setFrom(new InternetAddress(fromEmail))
    message.setRecipient(Message.RecipientType.TO, new InternetAddress(toEmail))
    message.setSubject("Alert Code Count Mismatch Detected")
    message.setText("The following alert codes have count mismatches:\n" + mismatchedAlertCodes.mkString("\n"))

    Transport.send(message)
    println("Mismatch email sent successfully.")
  }
}
