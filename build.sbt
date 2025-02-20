import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import java.util.{Date, Properties}
import javax.mail._
import javax.mail.internet._
import java.text.SimpleDateFormat

// Initialize Spark session
val spark = SparkSession.builder().appName("Alert Status Tracking").getOrCreate()

// Sample DataFrame (replace with actual data source)
val alertsDF = spark.read.format("jdbc")
  .option("url", "jdbc:mysql://your-db-host/your-db")
  .option("dbtable", "alerts_table")
  .option("user", "your-username")
  .option("password", "your-password")
  .load()

// Collections to track alert statuses
val successAlerts = mutable.Set[String]()
val failedAlerts = mutable.Map[String, String]() // Map to track failed alerts and reasons
val receivedAlerts = mutable.Set[String]() // Track all alerts received

// Process alerts in df.foreachPartition
alertsDF.foreachPartition(partition => {
  partition.foreach(row => {
    val alertCode = row.getAs[String]("alert_code")
    val dtCount = row.getAs[Int]("dt_count")
    val sourceCount = row.getAs[Int]("source_count")

    // Track received alerts
    receivedAlerts.synchronized { receivedAlerts += alertCode }

    // Validate and categorize alerts
    if (dtCount > 0) {
      if (sourceCount == dtCount) {
        successAlerts.synchronized { successAlerts += alertCode }
      } else {
        val reason = s"sourceCount ($sourceCount) != dtCount ($dtCount)"
        failedAlerts.synchronized { failedAlerts += (alertCode -> reason) }
      }
    } else {
      val reason = "dtCount is 0 or negative"
      failedAlerts.synchronized { failedAlerts += (alertCode -> reason) }
    }
  })
})

// Identify missed alerts (replace with actual expected alert logic)
val expectedDailyAlerts = alertsDF.filter(row => row.getAs[String]("frequency") == "daily")
val expectedAlertCodes = expectedDailyAlerts.select("alert_code").distinct().collect().map(_.getString(0)).toSet
val missedAlerts = expectedAlertCodes.diff(receivedAlerts)

// Format email body as HTML
val emailBodyHtml = s"""
  |<html>
  |<body>
  |<h2>Consolidated Alert Report</h2>
  |<h3>Success Alerts:</h3>
  |<table border="1" cellpadding="5" cellspacing="0">
  |  <tr><th>Alert Code</th></tr>
  |  ${successAlerts.map(code => s"<tr><td>$code</td></tr>").mkString("\n")}
  |</table>
  |
  |<h3>Failed Alerts:</h3>
  |<table border="1" cellpadding="5" cellspacing="0">
  |  <tr><th>Alert Code</th><th>Reason</th></tr>
  |  ${failedAlerts.map { case (code, reason) => s"<tr><td>$code</td><td>$reason</td></tr>" }.mkString("\n")}
  |</table>
  |
  |<
