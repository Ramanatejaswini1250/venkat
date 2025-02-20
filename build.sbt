
import java.time.LocalTime

def sendConsolidatedEmail(): Unit = {
  val currentTime = LocalTime.now()
  
  // Check if it's 4:00 PM
  if (currentTime.getHour == 16 && currentTime.getMinute == 0) {
    val emailContent = s"""
      <html>
        <body>
          $successSection
          $failedSection
          $missedSection
          <br><br>
          Thanks and Regards,<br>
          CDAO RAMP Team<br><br>
          *************** IMPORTANT MESSAGE ***************
        </body>
      </html>
    """.stripMargin

    sendEmail("CDAORiskAlertDeliveryTeam@cba.com.au", s"Consolidated Email for all Alerts on $currentDate", emailContent)
    println("Sending consolidated email...")
  } else {
    println("Skipping consolidated email, not 4 PM.")
  }
}
