import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Timer, TimerTask}

object RampAutomationExecution {

  def main(args: Array[String]): Unit = {
    scheduleConsolidatedEmail()
    spark.stop() // Ensure proper stopping of Spark
  }

  def scheduleConsolidatedEmail(): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateFormat.format(Calendar.getInstance().getTime)
    val flagFile = new File(s"/path/to/flags/consolidated_email_sent_$today.flag")

    // Check if the email has already been sent today
    if (flagFile.exists()) {
      println(s"Consolidated email already sent for $today. Skipping.")
      return
    }

    // Schedule the email for 4:00 PM AEST
    println(s"Scheduling consolidated email for 4:00 PM AEST on $today")
    val timer = new Timer()
    val scheduledTime = getScheduledTime(16, 0, 0) // 4:00 PM

    timer.schedule(new TimerTask {
      override def run(): Unit = {
        sendConsolidatedEmail()
        markEmailAsSent(flagFile)
      }
    }, scheduledTime)
  }

  def getScheduledTime(hour: Int, minute: Int, second: Int): java.util.Date = {
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.HOUR_OF_DAY, hour)
    calendar.set(Calendar.MINUTE, minute)
    calendar.set(Calendar.SECOND, second)
    calendar.getTime
  }

  def sendConsolidatedEmail(): Unit = {
    // Your email-sending logic
    println("Sending consolidated email...")
  }

  def markEmailAsSent(flagFile: File): Unit = {
    try {
      if (flagFile.createNewFile()) {
        println(s"Flag file created: ${flagFile.getAbsolutePath}")
      } else {
        println(s"Failed to create flag file: ${flagFile.getAbsolutePath}")
      }
    } catch {
      case e: Exception =>
        println(s"Error creating flag file: ${e.getMessage}")
    }
  }
}


