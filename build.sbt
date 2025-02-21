import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable
import java.util.{Calendar, Date, Properties}
import java.text.SimpleDateFormat
import javax.mail._
import javax.mail.internet._

// ----- Custom Accumulator for a Set[String] -----
class SetAccumulator extends AccumulatorV2[String, Set[String]] {
  private val _set: mutable.Set[String] = mutable.Set[String]()
  def isZero: Boolean = _set.isEmpty
  def copy(): AccumulatorV2[String, Set[String]] = {
    val newAcc = new SetAccumulator()
    newAcc._set ++= _set
    newAcc
  }
  def reset(): Unit = _set.clear()
  def add(v: String): Unit = _set += v
  def merge(other: AccumulatorV2[String, Set[String]]): Unit = other match {
    case o: SetAccumulator => _set ++= o._set
    case _ => throw new UnsupportedOperationException("Cannot merge with different type")
  }
  def value: Set[String] = _set.toSet
}

// ----- Custom Accumulator for Map[String, String] -----
class MapAccumulator extends AccumulatorV2[(String, String), Map[String, String]] {
  private val _map: mutable.Map[String, String] = mutable.Map[String, String]()
  def isZero: Boolean = _map.isEmpty
  def copy(): AccumulatorV2[(String, String), Map[String, String]] = {
    val newAcc = new MapAccumulator()
    newAcc._map ++= _map
    newAcc
  }
  def reset(): Unit = _map.clear()
  def add(v: (String, String)): Unit = _map += v
  def merge(other: AccumulatorV2[(String, String), Map[String, String]]): Unit = other match {
    case o: MapAccumulator => _map ++= o._map
    case _ => throw new UnsupportedOperationException("Cannot merge with different type")
  }
  def value: Map[String, String] = _map.toMap
}

// ----- Email Sending Function -----
def sendEmail(subject: String, bodyHtml: String): Unit = {
  val properties = new Properties()
  properties.put("mail.smtp.host", "smtp.your-email-provider.com")
  properties.put("mail.smtp.port", "587")
  properties.put("mail.smtp.auth", "true")

  val session = Session.getInstance(properties, new Authenticator() {
    override protected def getPasswordAuthentication: PasswordAuthentication =
      new PasswordAuthentication("your-email@example.com", "your-email-password")
  })

  val message = new MimeMessage(session)
  message.setFrom(new InternetAddress("your-email@example.com"))
  message.setRecipients(Message.RecipientType.TO, "cdao@gmail.com")
  message.setSubject(subject)
  message.setContent(bodyHtml, "text/html; charset=utf-8")
  Transport.send(message)
  println("Email sent successfully.")
}

// ----- Consolidated Mail Function -----
// This function builds the HTML email body using the aggregated results and sends the email.
def consolidatedMail(successAlerts: Set[String],
                     failedAlerts: Map[String, String],
                     missedAlerts: Set[String]): Unit = {
  val currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  val emailBodyHtml = s"""
    |<html>
    |<head>
    |  <style type="text/css">
    |    body { font-family: Arial, sans-serif; }
    |    h2 { color: #2E4053; }
    |    table { border-collapse: collapse; width: 100%; }
    |    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    |    th { background-color: #4CAF50; color: white; }
    |    .success { background-color: #d4edda; color: #155724; }
    |    .failed { background-color: #f8d7da; color: #721c24; }
    |    .missed { background-color: #fff3cd; color: #856404; }
    |    tr:hover { background-color: #f1f1f1; }
    |  </style>
    |</head>
    |<body>
    |  <h2>Consolidated Alert Report - $currentDate</h2>
    |
    |  <h3>Success Alerts</h3>
    |  <table>
    |    <tr><th>Alert Code</th></tr>
    |    ${if (successAlerts.isEmpty) "<tr><td>No successful alerts</td></tr>"
         else successAlerts.map(code => s"<tr class='success'><td>$code</td></tr>").mkString("\n")}
    |  </table>
    |
    |  <h3>Failed Alerts</h3>
    |  <table>
    |    <tr><th>Alert Code</th><th>Reason</th></tr>
    |    ${if (failedAlerts.isEmpty) "<tr><td colspan='2'>No failed alerts</td></tr>"
         else failedAlerts.map { case (code, reason) => s"<tr class='failed'><td>$code</td><td>$reason</td></tr>" }.mkString("\n")}
    |  </table>
    |
    |  <h3>Missed Alerts (Daily & Weekly)</h3>
    |  <table>
    |    <tr><th>Alert Code</th></tr>
    |    ${if (missedAlerts.isEmpty) "<tr><td>No missed alerts</td></tr>"
         else missedAlerts.map(code => s"<tr class='missed'><td>$code</td></tr>").mkString("\n")}
    |  </table>
    |
    |  <p><strong>Cutoff Time:</strong> 4:00 PM AEST</p>
    |</body>
    |</html>
  """.stripMargin
  sendEmail(s"Consolidated Alert Report - $currentDate", emailBodyHtml)
}

// ----- Main Application Object -----
object AlertReportApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Alert Status Tracking").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // Read the alerts DataFrame from a JDBC source (update connection details as needed)
    val alertsDF: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://your-db-host/your-db")
      .option("dbtable", "alerts_table")
      .option("user", "your-username")
      .option("password", "your-password")
      .load()

    // ----- Register Custom Accumulators on the Driver -----
    val successAcc = new SetAccumulator()
    spark.sparkContext.register(successAcc, "SuccessAcc")
    val failedAcc = new MapAccumulator()
    spark.sparkContext.register(failedAcc, "FailedAcc")
    val receivedAcc = new SetAccumulator()
    spark.sparkContext.register(receivedAcc, "ReceivedAcc")

    // ----- Process Data Using foreachPartition (runs on executors) -----
    alertsDF.foreachPartition { partition =>
      partition.foreach { row =>
        val alertCode = row.getAs[String]("alert_code")
        val dtCount = row.getAs[Int]("dt_count")
        val sourceCount = row.getAs[Int]("source_count")

        // Update the received alerts accumulator
        receivedAcc.add(alertCode)

        // Update success or failed accumulator based on validation logic
        if (dtCount > 0) {
          if (sourceCount == dtCount) {
            successAcc.add(alertCode)
          } else {
            val reason = s"sourceCount ($sourceCount) != dtCount ($dtCount)"
            failedAcc.add((alertCode, reason))
          }
        } else {
          val reason = "dtCount is 0 or negative"
          failedAcc.add((alertCode, reason))
        }
      }
    }

    // ----- Retrieve Aggregated Results on the Driver -----
    val combinedSuccessAlerts = successAcc.value
    val combinedFailedAlerts = failedAcc.value
    val combinedReceivedAlerts = receivedAcc.value

    // ----- Determine Expected Alerts for Frequencies "d" and "w" and Compute Missed Alerts -----
    val expectedAlerts: Set[String] = alertsDF
      .filter($"frequency".isin("d", "w"))
      .select("alert_code")
      .distinct()
      .as[String]
      .collect()
      .toSet

    val missedAlerts: Set[String] = expectedAlerts.diff(combinedReceivedAlerts)

    // ----- Check the Current Hour and Send Email Only Once per Day (e.g., at 4:00 PM) -----
    val calendar = Calendar.getInstance()
    val currentHour = calendar.get(Calendar.HOUR_OF_DAY)
    println(s"Current hour: $currentHour")
    if (currentHour == 16) {
      consolidatedMail(combinedSuccessAlerts, combinedFailedAlerts, missedAlerts)
    } else {
      println("Not the cutoff time. Email not sent.")
    }

    spark.stop()
  }
}
