import java.time.{ZoneId, ZonedDateTime, LocalTime}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// Create Spark session
val spark = SparkSession.builder()
  .appName("Alert Tracking and Email Notifications")
  .getOrCreate()

// Load etl_info table (contains expected alerts)
val etl_info = spark.read.table("etl_info_test")

// Load alerts data (processed alerts)
val df = spark.read.table("alert_load")

// ✅ Compute today's weekday number (1 = Sunday, ..., 7 = Saturday)
val todayNumber: Int = (ZonedDateTime.now(ZoneId.of("Australia/Sydney")).getDayOfWeek.getValue % 7) + 1

// ✅ Identify Expected Alerts (Daily + Weekly)
val expectedAlertsDF = etl_info
  .filter(col("frequency") === "d") // Daily alerts
  .select("alert_code")
  .distinct()
  .union(
    etl_info
      .filter(col("frequency") === "w" && col("run_days_dates") === todayNumber) // Weekly alerts for today
      .select("alert_code")
      .distinct()
  )
  .distinct()

val expectedAlerts: Set[String] = expectedAlertsDF
  .select(trim(upper(col("alert_code"))).alias("alert_code"))
  .collect()
  .map(_.getAs[String]("alert_code"))
  .toSet

// ✅ Identify Received Alerts (Processed Successfully Today)
val receivedAlertsDF = df
  .filter(col("dt_count") > 0) // Alerts that were processed successfully
  .select("alert_code")
  .distinct()

val receivedAlerts: Set[String] = receivedAlertsDF
  .select(trim(upper(col("alert_code"))).alias("alert_code"))
  .collect()
  .map(_.getAs[String]("alert_code"))
  .toSet

// ✅ Identify Alerts with Missing Frequency, Source Table, or Filter Column (Process Hourly)
val alertsWithIssuesDF = df
  .filter(col("dt_count") > 0 && (col("frequency").isNull || col("source_table_count").isNull || col("filter_column").isNull)) // Check for missing fields
  .select("alert_code", "frequency", "source_table_count", "filter_column", "business", "email_address")
  .distinct()

// ✅ Send Hourly Emails for Missing Fields
alertsWithIssuesDF.collect().foreach { row =>
  val alertCode = row.getAs[String]("alert_code")
  val frequencyMissing = row.getAs[String]("frequency") == null
  val sourceMissing = row.getAs[String]("source_table_count") == null
  val filterColumnMissing = row.getAs[String]("filter_column") == null
  val business = row.getAs[String]("business")
  val email = row.getAs[String]("email_address")

  val issueType = (frequencyMissing, sourceMissing, filterColumnMissing) match {
    case (true, false, false) => "Frequency Missing"
    case (false, true, false) => "Source Count Missing"
    case (false, false, true) => "Filter Column Missing"
    case (true, true, false)  => "Frequency and Source Count Missing"
    case (true, false, true)  => "Frequency and Filter Column Missing"
    case (false, true, true)  => "Source Count and Filter Column Missing"
    case (true, true, true)   => "All Three Missing (Frequency, Source, and Filter Column)"
    case _                    => "Unknown Issue"
  }

  val emailSubject = s"⚠️ Alert Issue: $alertCode - $issueType"
  val emailBody =
    s"""
      |Hello $business Team,
      |
      |The alert **$alertCode** has been processed but is missing critical information:
      |🔹 **Issue:** $issueType
      |
      |Please investigate and ensure the data is complete.
      |
      |Best regards,  
      |Automation Team
    """.stripMargin

  sendEmail(email, emailSubject, emailBody)
  println(s"📧 Sent issue email to $email for alert code: $alertCode ($issueType)")
}

// ✅ Compute Fully Missed Alerts (ONLY at 4 PM)
val currentTime = LocalTime.now(ZoneId.of("Australia/Sydney"))

// Run fully missed alert check ONLY at 4 PM AEST
if (currentTime.getHour == 16 && currentTime.getMinute == 0) {
  val fullyMissedAlerts: Set[String] = expectedAlerts.diff(receivedAlerts)

  if (fullyMissedAlerts.nonEmpty) {
    val businessContactsDF = etl_info
      .filter(col("alert_code").isin(fullyMissedAlerts.toSeq: _*)) // Fetch business contacts
      .select("alert_code", "business", "email_address")
      .distinct()
      .collect()

    businessContactsDF.foreach { row =>
      val alertCode = row.getAs[String]("alert_code")
      val business = row.getAs[String]("business")
      val email = row.getAs[String]("email_address")

      val emailSubject = s"🔴 Missed Alert Notification: $alertCode"
      val emailBody =
        s"""
          |Hello $business Team,
          |
          |The alert **$alertCode** was scheduled for today but did not arrive by the 4:00 PM AEST cutoff.
          |Please investigate the issue.
          |
          |Best regards,  
          |Automation Team
        """.stripMargin

      sendEmail(email, emailSubject, emailBody)
      println(s"📧 Sent missed alert email to $email for alert code: $alertCode")
    }
  }

  // ✅ Send Consolidated Email at 4:05 PM AEST
  val consolidatedEmailSubject = "📊 Daily Alert Status Report - 4:00 PM AEST"

  val consolidatedEmailBody =
    s"""
      |Hello Team,
      |
      |Below is the **daily summary** of alerts as of 4:00 PM AEST:
      |
      |✅ **Received Alerts**: ${receivedAlerts.mkString(", ")}
      |⚠️ **Alerts with Issues**: ${alertsWithIssuesDF.select("alert_code").collect().map(_.getString(0)).mkString(", ")}
      |🔴 **Missed Alerts**: ${fullyMissedAlerts.mkString(", ")}
      |
      |Please take necessary actions where required.
      |
      |Best regards,  
      |Automation Team
    """.stripMargin

  sendEmail("cdao@gmail.com", consolidatedEmailSubject, consolidatedEmailBody)

  println(s"📧 Sent consolidated email with all alert statuses.")
}
