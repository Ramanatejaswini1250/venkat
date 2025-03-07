import java.time.LocalDate
import org.apache.spark.sql.functions._

// Get today's day number dynamically (Sunday = 1, Monday = 2, ..., Saturday = 7)
val todayNumber: Int = LocalDate.now().getDayOfWeek.getValue % 7 + 1

// ✅ Step 1: Identify Expected Alerts (Daily + Weekly Based on `run_days_dates`)
val expectedAlertsDF = df
  .filter(col("frequency") === "d")
  .select("alert_code")
  .distinct()
  .union(
    etl_info
      .filter(col("frequency") === "w" && col("run_days_dates") === todayNumber) // Match today's number
      .select("alert_code")
      .distinct()
  )
  .distinct() // Remove duplicates

val expectedAlerts: Set[String] = expectedAlertsDF.collect().map(_.getAs[String]("alert_code")).toSet

// ✅ Step 2: Identify Received Alerts (Processed with `dt_count > 0`)
val receivedAlerts: Set[String] = df
  .filter(col("dt_count") > 0)
  .select("alert_code")
  .distinct()
  .collect()
  .map(_.getAs[String]("alert_code"))
  .toSet

// ✅ Step 3: Compute Missed Alerts (Expected - Received)
val missedAlerts: Set[String] = expectedAlerts.diff(receivedAlerts)
println(s"🔴 Missed Alerts at Cutoff Time: ${missedAlerts.mkString(", ")}")

// ✅ Step 4: Identify Issues (Frequency Missing or Source Data Missing)
val issueAlertsDF = df
  .filter(col("alert_code").isin(missedAlerts.toSeq: _*))
  .select("alert_code", "frequency", "source_table")
  .distinct()
  .collect()

val frequencyMissed = issueAlertsDF.filter(row => row.getAs[String]("frequency") == null).map(_.getAs[String]("alert_code")).toSet
val sourceTableMissed = issueAlertsDF.filter(row => row.getAs[String]("source_table") == null).map(_.getAs[String]("alert_code")).toSet

// ✅ Step 5: Send Individual Emails for Missed Alerts
if (missedAlerts.nonEmpty) {
  val businessContactsDF = df
    .filter(col("alert_code").isin(missedAlerts.toSeq: _*))
    .select("alert_code", "business", "email_address")
    .distinct()
    .collect()

  businessContactsDF.foreach { row =>
    val alertCode = row.getAs[String]("alert_code")
    val business = row.getAs[String]("business")
    val email = row.getAs[String]("email_address")

    val issueType = 
      if (frequencyMissed.contains(alertCode)) "⚠️ Frequency Missing"
      else if (sourceTableMissed.contains(alertCode)) "⚠️ Source Data Missing"
      else "🚨 Alert Not Received"

    val emailSubject = s"🔴 $issueType Notification: $alertCode"
    val emailBody =
      s"""
        |Hello $business Team,
        |
        |The alert **$alertCode** was expected today but did not arrive.
        |Issue Type: **$issueType**
        |
        |Please investigate the issue.
        |
        |Best regards,  
        |Automation Team
      """.stripMargin

    sendEmail(email, emailSubject, emailBody)
    println(s"📧 Sent missed alert email to $email for alert code: $alertCode with issue: $issueType")
  }
}

// ✅ Step 6: Include Missed Alerts in the 4:00 PM Consolidated Email
sendConsolidatedEmail(successAlerts, failureAlerts, missedAlerts, frequencyMissed, sourceTableMissed)
