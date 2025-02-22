import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

// Custom SetAccumulator for collecting unique alert codes
class SetAccumulator[T] extends AccumulatorV2[T, Set[T]] {
  private val set = mutable.Set[T]()

  override def isZero: Boolean = set.isEmpty
  override def copy(): AccumulatorV2[T, Set[T]] = {
    val newAcc = new SetAccumulator[T]()
    newAcc.set ++= this.set
    newAcc
  }
  override def reset(): Unit = set.clear()
  override def add(v: T): Unit = set += v
  override def merge(other: AccumulatorV2[T, Set[T]]): Unit = set ++= other.value
  override def value: Set[T] = set.toSet
}

// Initialize Spark Session
val spark = SparkSession.builder()
  .appName("Success Alerts Email")
  .master("yarn")  // Adjust based on your cluster setup
  .getOrCreate()

import spark.implicits._

// Register the accumulator
val successAlertsAccumulator = new SetAccumulator[String]()
spark.sparkContext.register(successAlertsAccumulator, "SuccessAlertsAccumulator")

// Function to check if an alert was successfully processed
def isAlertSuccessful(alertCode: String): Boolean = {
  // Implement logic to determine if an alert is successful
  true  // Placeholder: Replace with actual success condition
}

// Function to send success email
def sendSuccessEmail(successAlerts: Set[String]): Unit = {
  val currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  val alertCount = successAlerts.size
  val emailSubject = if (alertCount > 0) s"Success Alerts - $currentDate ($alertCount alerts)" 
                     else s"Success Alerts - $currentDate"

  val alertRows = if (successAlerts.isEmpty) {
    """<tr><td colspan="1" style="text-align:center; font-style:italic; color:gray;">No successful alerts</td></tr>"""
  } else {
    successAlerts.map(code => s"<tr class='success'><td>$code</td></tr>").mkString("\n")
  }

  val emailBodyHtml = s"""
    |<html>
    |<head>
    |  <style type="text/css">
    |    body { font-family: Arial, sans-serif; background-color: #f9f9f9; margin: 0; padding: 0; }
    |    .container { max-width: 600px; margin: 0 auto; background-color: #fff; padding: 10px 20px; border-radius: 8px; box-shadow: 2px 2px 10px rgba(0,0,0,0.1); }
    |    h2 { color: #2E4053; margin-top: 10px; }
    |    p { font-size: 14px; margin: 5px 0; }
    |    table { border-collapse: collapse; width: 100%; margin-bottom: 10px; font-size: 14px; border-radius: 5px; overflow: hidden; }
    |    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    |    th { background-color: #4CAF50; color: white; font-weight: 600; }
    |    .success { background-color: #d4edda; color: #155724; font-weight: bold; }
    |    tr:hover { background-color: #f1f1f1; }
    |  </style>
    |</head>
    |<body>
    |  <div class="container">
    |    <p>Hi Team,</p>
    |    <p>Please find below the list of successfully processed alerts for <strong>$currentDate</strong>:</p>
    |    <h2>Success Alerts</h2>
    |    <table>
    |      <tr><th>Alert Code</th></tr>
    |      $alertRows
    |    </table>
    |    <p><strong>Cutoff Time:</strong> 4:00 PM AEST</p>
    |  </div>
    |</body>
    |</html>
  """.stripMargin

  try {
    sendEmail(emailSubject, emailBodyHtml)
  } catch {
    case e: Exception => println(s"Failed to send success email: ${e.getMessage}")
  }
}

// Simulated send email function
def sendEmail(subject: String, body: String): Unit = {
  println(s"Email Sent: $subject")
}

// Assume df contains alert data
val df = spark.read.format("csv").option("header", "true").load("path/to/alerts.csv")

// Using foreachPartition to avoid multiple email triggers
df.foreachPartition(partition => {
  partition.foreach(row => {
    val alertCode = row.getAs[String]("alert_code")
    if (isAlertSuccessful(alertCode)) {
      successAlertsAccumulator.add(alertCode)  // Collect successful alerts in accumulator
    }
  })
})

// Fetch all collected alerts from the accumulator
val successAlerts = successAlertsAccumulator.value

// Send email only once with all success alerts
if (successAlerts.nonEmpty) {
  sendSuccessEmail(successAlerts)
}

spark.stop()
