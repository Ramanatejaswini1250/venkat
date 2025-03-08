import java.time.{LocalTime, ZoneId}
import org.apache.spark.sql.functions._
import scala.concurrent._
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable

// ✅ Get current time in Australia/Sydney
val currentTime = LocalTime.now(ZoneId.of("Australia/Sydney"))

// ✅ Expected Alerts (From ETL_INFO)
val expectedAlertsDF = etl_info
  .select("alert_code", "frequency", "business", "email_address", "source_table_column")

val expectedAlerts = expectedAlertsDF.collect().map(_.getString(0)).toSet

// ✅ Tracking Variables
val processedAlerts = mutable.Set[String]()
val successAlerts = mutable.Set[String]()
val failedAlerts = mutable.Set[String]()
val alertsWithIssues = mutable.Set[String]()
val processingCompleted = new AtomicBoolean(false)

// ✅ Process Alerts Inside df.foreachPartition
df.foreachPartition { partition =>
  val conn = getJDBCConnection()
  
  partition.foreach { row =>
    val alertCode = row.getAs[String]("alert_code")
    val frequency = row.getAs[String]("frequency")
    val sourceTableColumn = row.getAs[String]("source_table_column")
    val business = row.getAs[String]("business")
    val emailAddress = row.getAs[String]("email_address")
    val dtCount = row.getAs[Int]("dt_count")
    val sourceCount = row.getAs[Int]("sourcecount")
    val masterCount = row.getAs[Int]("mastercount")
    val csvGenerated = row.getAs[Boolean]("csv_generated")

    // ✅ Mark as processed
    processedAlerts.add(alertCode)

    // ✅ If missing frequency or source_table_column, send an immediate issue email
    if (frequency == null || sourceTableColumn == null) {
      alertsWithIssues.add(alertCode) // Track for 4 PM check
      
      val subject = s"⚠️ Alert Issue: $alertCode"
      val emailBody = s"""
        |The alert <b>$alertCode</b> is missing important information.
        |<b>Issue:</b> Frequency or Source Table Column is missing.
        |
        |Please fix this before 4 PM AEST cutoff.
      """.stripMargin

      sendEmail(emailAddress, subject, emailBody, "")
      println(s"✅ Sent issue email to $emailAddress for alert code: $alertCode")
    }

    // ✅ Categorize alerts into success and failed alerts
    if (dtCount > 0 && sourceCount == dtCount && masterCount == dtCount && csvGenerated) {
      successAlerts.add(alertCode)
    } else {
      failedAlerts.add(alertCode)
    }
  }
  
  conn.close()
}

// ✅ Mark processing as completed
processingCompleted.set(true)

// ✅ Step 1: Send Hourly Email With Success & Failed Alerts
if (currentTime.getMinute == 0) { // Runs every hour
  val successList = successAlerts.mkString(", ")
  val failedList = failedAlerts.mkString(", ")

  val hourlyBody = s"""
    |<h3>⏳ Hourly Alert Processing Update</h3>
    |
    |<b>✅ Success Alerts:</b> $successList
    |
    |<b>❌ Failed Alerts:</b> $failedList
    |
    |Next update will be sent in 1 hour.
  """.stripMargin

  sendEmail("cdao@gmail.com", "⏳ Hourly Alert Status", hourlyBody, "")
  println("✅ Sent Hourly Email with Success & Failed Alerts.")
}

// ✅ Step 2: Wait for Processing to Complete Before Sending 4 PM Email
if (currentTime.getHour == 16 && currentTime.getMinute == 0) {
  
  println("⏳ Waiting for alerts to finish processing before sending consolidated email...")

  // Wait up to 5 minutes for processing to complete
  Await.result(Future {
    while (!processingCompleted.get()) {
      Thread.sleep(5000) // Check every 5 seconds
    }
  }, 5.minutes)

  println("✅ Alert processing complete. Sending 4 PM email.")

  // ✅ Get processed alerts from alert_load_archive
  val processedAlertsDF = spark.read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", "alert_load_archive") 
    .option("user", dbUser)
    .option("password", dbPassword)
    .load()
    .select("alert_code")
    .distinct()

  val finalProcessedAlerts = processedAlertsDF.collect().map(_.getString(0)).toSet ++ processedAlerts

  // ✅ Alerts that never arrived
  val missedAlerts = expectedAlerts.diff(finalProcessedAlerts)

  // ✅ Include unresolved missing info alerts (not fixed by 4 PM)
  val unresolvedIssues = alertsWithIssues.diff(finalProcessedAlerts)
  val finalMissedAlerts = (missedAlerts ++ unresolvedIssues).toSeq.distinct

  // ✅ Send Missed Alert Emails to Business
  if (finalMissedAlerts.nonEmpty) {
    val missedAlertsDF = finalMissedAlerts.toDF("alert_code")
    
    val businessContactsDF = df
      .join(missedAlertsDF, Seq("alert_code"), "inner")
      .select("alert_code", "business", "email_address")
      .distinct()
      .collect()

    businessContactsDF.foreach { row =>
      val alertCode = row.getAs[String]("alert_code")
      val business = row.getAs[String]("business")
      val emailAddress = row.getAs[String]("email_address")

      val subject = s"🔴 Missed Alert Notification: $alertCode"
      val emailBody = s"""
        |The alert <b>$alertCode</b> was scheduled for today but did not arrive by the 4:00 PM AEST cutoff.
        |
        |Could you please check and confirm if this is expected?
      """.stripMargin

      sendEmail(emailAddress, subject, emailBody, "")
      println(s"✅ Sent missed alert email to $emailAddress for alert code: $alertCode")
    }
  }

  // ✅ Step 3: Send Final 4 PM Consolidated Email to Internal Team
  val consolidatedBody = s"""
    |<h3>🔔 Consolidated Alert Summary (4:00 PM AEST)</h3>
    |
    |<b>✅ Success Alerts:</b> ${finalProcessedAlerts.mkString(", ")}
    |
    |<b>❌ Failed Alerts:</b> ${failedAlerts.mkString(", ")}
    |
    |<b>⏳ Missed Alerts:</b> ${finalMissedAlerts.mkString(", ")}
    |
    |Some alerts had missing data and were not fixed by 4 PM.
  """.stripMargin

  sendEmail("cdao@gmail.com", "🚨 Daily Consolidated Alert Status", consolidatedBody, "")
  println("✅ Sent Final Consolidated Alert Email.")
}
