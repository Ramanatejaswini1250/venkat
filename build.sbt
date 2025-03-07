// ⏳ Step 1: Introduce delay before computing missed alerts
Thread.sleep(120000) // 2-minute delay before processing

// ✅ Step 2: Identify expected alerts based on frequency (Daily, Weekly, Monthly, Quarterly)
val expectedAlerts: Set[String] = df
  .filter(col("frequency").isin("d", "w", "m", "q")) // Only scheduled alerts
  .select("alert_code")
  .distinct()
  .collect()
  .map(_.getAs[String]("alert_code"))
  .toSet

// ✅ Step 3: Identify received alerts (Processed alerts with dt_count > 0)
val receivedAlerts: Set[String] = df
  .filter(col("dt_count") > 0) // Processed alerts only
  .select("alert_code")
  .distinct()
  .collect()
  .map(_.getAs[String]("alert_code"))
  .toSet

// ✅ Step 4: Compute Fully Missed Alerts (Expected - Received)
val fullyMissedAlerts: Set[String] = expectedAlerts.diff(receivedAlerts)

// ✅ Step 5: Identify Alerts with Missing Frequency or Source Data
val processedAlertsWithIssues: Set[String] = df
  .filter(col("dt_count") > 0)
  .filter(col("frequency").isNull || col("source_table").isNull) // Check missing frequency or source data
  .select("alert_code")
  .distinct()
  .collect()
  .map(_.getAs[String]("alert_code"))
  .toSet

println(s"🔴 Fully Missed Alerts: ${fullyMissedAlerts.mkString(", ")}")
println(s"⚠️ Processed Alerts with Issues: ${processedAlertsWithIssues.mkString(", ")}")

// 📧 Step 6: Send Individual Emails for Missed Alerts
val missedAlertsToNotify = fullyMissedAlerts ++ processedAlertsWithIssues

if (missedAlertsToNotify.nonEmpty) {
  val businessContactsDF = df
    .where(col("alert_code").isin(missedAlertsToNotify.toSeq: _*)) // Efficient filtering before collect
    .select("alert_code", "business", "email_address")
    .distinct()
    .collect()

  businessContactsDF.foreach { row =>
    val alertCode = row.getAs[String]("alert_code")
    val business = row.getAs[String]("business")
    val email = row.getAs[String]("email_address")

    val isFullyMissed = fullyMissedAlerts.contains(alertCode)
    val isIssueInData = processedAlertsWithIssues.contains(alertCode)

    val issueDetails = if (isFullyMissed) {
      "Alert was expected but did not arrive at the cutoff time."
    } else if (isIssueInData) {
      "Alert was processed but has missing frequency or source table data."
    } else {
      "Unknown issue."
    }

    val emailSubject = s"🔴 Alert Issue Notification: $alertCode"
    val emailBody =
      s"""
        |Hello $business Team,
        |
        |The alert with code **$alertCode** has an issue:
        |$issueDetails
        |
        |Please investigate and resolve this issue.
        |
        |Best regards,  
        |Automation Team
      """.stripMargin

    sendEmail(email, emailSubject, emailBody)
    println(s"📧 Sent alert issue email to $email for alert code: $alertCode")
  }
}

// 📨 Step 7: Send Consolidated Email with Categorized Missed & Issue Alerts
val emailBody =
  s"""
    |🔴 **Missed & Issue Alerts Summary (4:00 PM AEST)**
    |
    |**Fully Missed Alerts:** ${fullyMissedAlerts.mkString(", ")}
    |**Alerts Processed but Missing Data:** ${processedAlertsWithIssues.mkString(", ")}
    |
    |Best regards,  
    |Automation Team
  """.stripMargin

sendConsolidatedEmail("Missed & Issue Alerts Summary", emailBody)
