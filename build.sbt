import java.io.{BufferedReader, FileReader}
import java.util.Properties
import javax.mail.{Message, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}
import scala.collection.mutable
import scala.io.Source

object AlertCodeValidator {

  def main(args: Array[String]): Unit = {
    val csvFilePath = "/path/to/master1.csv"
    val masterTableFilePath = "/path/to/masterTable1.csv"

    // Step 1: Read CSV file and count alert codes
    val csvAlertCodeCountMap = readCsvAlertCodeCounts(csvFilePath)

    // Step 2: Read master table CSV file and count alert codes
    val masterAlertCodeCountMap = readCsvAlertCodeCounts(masterTableFilePath)

    // Step 3: Compare CSV counts with master table counts
    val mismatchedAlertCodes = mutable.ArrayBuffer[String]()

    csvAlertCodeCountMap.foreach { case (alertCode, csvCount) =>
      val masterCount = masterAlertCodeCountMap.getOrElse(alertCode, 0)
      if (csvCount != masterCount) {
        mismatchedAlertCodes += s"Alert Code: $alertCode, CSV Count: $csvCount, Master Count: $masterCount"
      }
    }

    // Step 4: Send email or print success
    if (mismatchedAlertCodes.isEmpty) {
      println("All alert counts match successfully.")
    } else {
      println("Mismatched Alert Codes Found:")
      mismatchedAlertCodes.foreach(println)
      sendEmail(mismatchedAlertCodes)
    }
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
