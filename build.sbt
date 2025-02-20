import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import java.util.{Date, Properties, Calendar}
import javax.mail._
import javax.mail.internet._
import java.text.SimpleDateFormat

// Initialize Spark session
val spark = SparkSession.builder().appName("Alert Status Tracking").getOrCreate()

// Sample DataFrame (replace with your actual DataFrame)
val alertsDF = spark.read.format("jdbc")
  .option("url", "jdbc:mysql://your-db-host/your-db")
  .option("dbtable", "alerts_table")
  .option("user", "your-username")
  .option("password", "your-password")
  .load()

// Collections to track alert statuses
val successAlerts = mutable.Set[String]()
val failedAlerts = mutable.Set[String]()
val receivedAlerts = mutable.Set[String]() // Track all alerts received

// Use synchronized blocks to ensure thread safety when updating shared collections
alertsDF.foreachPartition(partition => {
  partition.foreach(row => {
    val alertCode = row.getAs[String]("alert_code")
    val dtCount = row.getAs[Int]("dt_count")
    val sourceCount = row.getAs[Int]("source_count")

    // Track received alerts
    receivedAlerts.synchronized {
      receivedAlerts += alertCode
    }

    // Check validation conditions
    if (dtCount > 0) {
      if (sourceCount == dtCount) {
        // Simulate successful execution of alertCode.sql
        println(s"Executing $alertCode.sql")
        successAlerts.synchronized {
          successAlerts += alertCode
        }
      } else {
        println(s"Validation failed for $alertCode: sourceCount ($sourceCount) != dtCount ($dtCount)")
        failedAlerts.synchronized {
          failedAlerts += alertCode
        }
      }
    }
  })
})

// Identify expected daily alerts (replace with your actual criteria)
val expectedDailyAlerts = alertsDF.filter(row => row.getAs[String]("frequency") == "daily")
val expectedAlertCodes = expectedDailyAlerts.select("alert_code").distinct().collect().map(_.getString(0)).toSet

// Identify missed alerts (alerts expected but not received)
val missedAlerts = expectedAlertCodes.diff(receivedAlerts)

// Consolidate alert statuses
val currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
val emailBody = s"""
  |Consolidated Alert Report:
  |
  |Success Alerts:
  |${successAlerts.mkString(", ")}
  |
  |Failed Alerts:
  |${failedAlerts.mkString(", ")}
  |
  |Missed Alerts:
  |${missedAlerts.mkString(", ")}
  |
  |Cutoff Time: 4:00 PM AEST
""".stripMargin

// Email sending function
def sendEmail(subject: String, body: String): Unit = {
  val properties = new Properties()
  properties.put("mail.smtp.host", "smtp.your-email-provider.com")
  properties.put("mail.smtp.port", "587")
  properties.put("mail.smtp.auth", "true")

  val session = Session.getInstance(properties, new javax.mail.Authenticator() {
    override protected def getPasswordAuthentication: PasswordAuthentication =
      new PasswordAuthentication("your-email@example.com", "your-email-password")
  })

  val message = new MimeMessage(session)
  message.setFrom(new InternetAddress("your-email@example.com"))
  message.setRecipients(Message.RecipientType.TO, "cdao@gmail.com")
  message.setSubject(subject)
  message.setText(body)

  Transport.send(message)
  println("Email sent successfully.")
}

// Send the consolidated email at the cutoff time
sendEmail(s"Consolidated Alert Report - $currentDate", emailBody)
