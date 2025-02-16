import java.util.concurrent.ConcurrentHashMap
import java.util.{Calendar, Timer, TimerTask}
import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}

// Shared collections for alert statuses
val successAlerts = ConcurrentHashMap.newKeySet[String]()       // Collects successful alert codes
val failedAlerts = new ConcurrentHashMap[String, String]()      // Collects failed alert codes with reasons
var missedAlerts: Set[String] = Set()                           // Captures missed alerts

// Step 1: Function to send an immediate failure email
def sendFailureEmail(alertCode: String, reason: String): Unit = {
  val subject = s"Immediate Alert Failure: $alertCode"
  val body =
    s"""
       |<html>
       |<body>
       |  <h2 style="color:red;">Alert Validation Failed</h2>
       |  <p><b>Alert Code:</b> $alertCode</p>
       |  <p><b>Reason:</b> $reason</p>
       |</body>
       |</html>
     """.stripMargin

  // Replace with actual email-sending logic
  println(s"Sending immediate failure email for $alertCode")
  println(body)
}

// Step 2: Function to send the consolidated email
def sendConsolidatedEmail(): Unit = {
  // Build the "Success Alerts" table
  val successSection = if (successAlerts.isEmpty) {
    "<p>No success alerts.</p>"
  } else {
    val header = "<h3 style='color:green;'>Success Alerts:</h3><table border='1'><tr><th>Alert Code</th></tr>"
    val rows = successAlerts.asScala.map(alertCode => s"<tr><td>$alertCode</td></tr>").mkString("")
    s"$header$rows</table>"
  }

  // Build the "Failed Alerts" table
  val failedSection = if (failedAlerts.isEmpty) {
    "<p>No failed alerts.</p>"
  } else {
    val header = "<h3 style='color:red;'>Failed Alerts:</h3><table border='1'><tr><th>Alert Code</th><th>Reason</th></tr>"
    val rows = failedAlerts.asScala.map { case (alertCode, reason) => s"<tr><td>$alertCode</td><td>$reason</td></tr>" }.mkString("")
    s"$header$rows</table>"
  }

  // Build the "Missed Alerts" table
  val missedSection = if (missedAlerts.isEmpty) {
    "<p>No missed alerts.</p>"
  } else {
    val header = "<h3 style='color:orange;'>Missed Alerts:</h3><table border='1'><tr><th>Alert Code</th></tr>"
    val rows = missedAlerts.map(alertCode => s"<tr><td>$alertCode</td></tr>").mkString("")
    s"$header$rows</table>"
  }

  // Combine all sections into the email body
  val emailContent =
    s"""
       |<html>
       |<body>
       |  <h2>Consolidated Alert Status (Daily/Weekly):</h2>
       |  $successSection
       |  $failedSection
       |  $missedSection
       |</body>
       |</html>
     """.stripMargin

  // Replace with actual email-sending logic
  println("Sending consolidated email...")
  println(emailContent)
}

// Step 3: Function to collect missed alerts
def collectMissedAlerts(df: DataFrame): Set[String] = {
  // Get all alerts with frequency 'd' or 'w' from the joined dataframe
  val scheduledAlerts = df.filter($"frequency".isin("d", "w"))
    .select("alert_code")
    .distinct()
    .collect()
    .map(_.getString(0))
    .toSet

  // Combine success and failed alerts to get all processed alerts
  val processedAlerts = successAlerts.asScala.toSet ++ failedAlerts.keySet().asScala.toSet

  // Missed alerts are those scheduled but not processed
  scheduledAlerts.diff(processedAlerts)
}

// Step 4: Main method to coordinate the process
def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder.appName("Alert Processing").getOrCreate()

  // Join `alert_load` and `etl_info` to get the full dataframe (df)
  val df = joinAlertLoadAndEtlInfo()

  // Process alerts in `df.foreachPartition`
  df.foreachPartition { partition =>
    partition.foreach { row =>
      val alertCode = row.getAs[String]("alert_code")
      val dtCount = row.getAs[Int]("dt_count")
      val sourceCount = row.getAs[Int]("sourcecount")
      val masterCount = row.getAs[Int]("mastercount")

      if (dtCount > 0 && sourceCount == dtCount) {
        try {
          // Run SQL script and generate CSVs
          runSqlScriptForAlert(alertCode)
          if (masterCount == dtCount) {
            generateMasterCSVFiles(alertCode)
            archiveAlert(alertCode)  // Archive the alert
            successAlerts.add(alertCode)  // Mark as success
          } else {
            val reason = "Master count mismatch"
            failedAlerts.put(alertCode, reason)
            sendFailureEmail(alertCode, reason)  // Send immediate failure email
          }
        } catch {
          case e: Exception =>
            val reason = s"Error: ${e.getMessage}"
            failedAlerts.put(alertCode, reason)
            sendFailureEmail(alertCode, reason)  // Send immediate failure email
        }
      } else {
        val reason = "Validation failed (dtCount or sourceCount mismatch)"
        failedAlerts.put(alertCode, reason)
        sendFailureEmail(alertCode, reason)  // Send immediate failure email
      }
    }
  }

  // Collect missed alerts after processing all partitions
  missedAlerts = collectMissedAlerts(df)

  // Schedule the consolidated email at 4:00 PM
  val timer = new Timer()
  val calendar = Calendar.getInstance()
  calendar.set(Calendar.HOUR_OF_DAY, 16) // 4:00 PM
  calendar.set(Calendar.MINUTE, 0)
  calendar.set(Calendar.SECOND, 0)

  // Schedule the email task
  timer.schedule(new TimerTask {
    override def run(): Unit = sendConsolidatedEmail()
  }, calendar.getTime(), 24 * 60 * 60 * 1000) // Repeat daily
}
