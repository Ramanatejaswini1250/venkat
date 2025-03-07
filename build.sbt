// Step 1: Identify expected alerts
val expectedAlerts: Set[String] = df
  .filter(col("frequency").isin("d", "w", "m", "q")) // Consider all scheduled frequencies
  .select("alert_code")
  .distinct()
  .collect()
  .map(row => row.getAs[String]("alert_code"))
  .toSet

// Step 2: Identify received alerts (processed with dt_count > 0)
val receivedAlerts: Set[String] = df
  .filter(col("dt_count") > 0) // Only consider alerts that were actually processed
  .select("alert_code")
  .distinct()
  .collect()
  .map(row => row.getAs[String]("alert_code"))
  .toSet

// Step 3: Compute missed alerts (Expected - Received)
val missedAlerts: Set[String] = expectedAlerts.diff(receivedAlerts)

println(s"🔴 Missed Alerts at Cutoff Time: ${missedAlerts.mkString(", ")}")

// Step 4: Introduce a delay before final processing (ensure last-minute updates are captured)
Thread.sleep(120000) // 2-minute delay before marking alerts as missed

// Step 5: Send individual emails for each missed alert
if (missedAlerts.nonEmpty) {
  val businessContactsDF = df
    .filter(col("alert_code").isin(missedAlerts.toSeq: _*)) // Fetch business contacts for missed alerts
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
        |The alert with code **$alertCode** was expected but did not arrive by the cutoff time (4:00 PM AEST).
        |Please investigate the issue.
        |
        |Best regards,  
        |Automation Team
      """.stripMargin

    sendEmail(email, emailSubject, emailBody)
    println(s"📧 Sent missed alert email to $email for alert code: $alertCode")
  }
}

// Step 6: Send consolidated email including missed alerts
sendConsolidatedEmail(successAlerts, failureAlerts, missedAlerts)
