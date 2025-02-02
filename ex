import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.concurrent.duration._

object ExecutionTracker {
  private val executionStatus = new ConcurrentHashMap[String, Boolean]().asScala

  def markCompleted(alertCode: String): Unit = {
    executionStatus(alertCode) = true
  }

  def isCompleted(alertCode: String): Boolean = {
    executionStatus.getOrElse(alertCode, false)
  }
}

// Simulated runSqlScript method (Replace with actual implementation)
def runSqlScript(alertCode: String): Boolean = {
  println(s"Executing SQL scripts for alertCode = $alertCode...")
  Thread.sleep(5000) // Simulating execution time
  ExecutionTracker.markCompleted(alertCode) // Mark execution as complete
  println(s"SQL scripts completed for alertCode = $alertCode")
  true
}

def waitForRunSqlScriptCompletion(alertCode: String): Boolean = {
  println(s"Waiting for runSqlScript to complete for alertCode = $alertCode...")

  var retries = 0
  val maxRetries = 10
  val retryInterval = 10000 // 10 seconds

  while (retries < maxRetries) {
    if (ExecutionTracker.isCompleted(alertCode)) {
      println(s"SQL script execution completed successfully for alertCode = $alertCode.")
      return true
    } else {
      println(s"runSqlScript not yet completed for alertCode = $alertCode. Retrying in ${retryInterval / 1000} seconds...")
      Thread.sleep(retryInterval)
      retries += 1
    }
  }

  println(s"runSqlScript did not complete within the expected time for alertCode = $alertCode.")
  false
}

def waitForDataToLoadAndValidate(
    alertCode: String,
    countQuery1: String,
    countQuery2: String,
    dtCount: Long,
    maxRetries: Int = 10,
    retryInterval: Long = 10000
): Boolean = {
  if (!waitForRunSqlScriptCompletion(alertCode)) {
    println(s"Skipping validation as SQL script execution failed for alertCode = $alertCode.")
    return false
  }

  println(s"Starting validation for alertCode = $alertCode after SQL execution.")

  var retries = 0
  var dataLoaded = false

  while (retries < maxRetries && !dataLoaded) {
    println(s"Attempt ${retries + 1}: Checking data for alertCode = $alertCode")

    val count1Opt = executeCountQuery(countQuery1)
    count1Opt match {
      case Some(count1) if count1 == dtCount =>
        println(s"Count1 matched dtCount for alertCode = $alertCode: $count1")
        
        val count2Opt = executeCountQuery(countQuery2)
        count2Opt match {
          case Some(count2) if count2 > 0 =>
            println(s"Data successfully loaded for alertCode = $alertCode: count1 = $count1, count2 = $count2")
            dataLoaded = true
          case _ =>
            println(s"Data for alertCode = $alertCode is not yet available in table2.")
        }
        
      case _ =>
        println(s"Count1 does not match dtCount or is not available for alertCode = $alertCode.")
    }

    if (!dataLoaded) {
      retries += 1
      if (retries < maxRetries) {
        println(s"Retrying in ${retryInterval / 1000} seconds...")
        Thread.sleep(retryInterval)
      }
    }
  }

  if (!dataLoaded) {
    println(s"Data load failed for alertCode = $alertCode after $maxRetries retries.")
  }

  dataLoaded
}

// Now, calling this in the `df.foreachPartition` for master1 and master2 logic
df.foreachPartition { partition =>
  partition.foreach { row =>
    val alertCode = row.getAs[String]("alertCode")

    val countQuery1 = s"SELECT COUNT(*) FROM master1 WHERE alert_code = '$alertCode'"
    val countQuery2 = s"SELECT COUNT(*) FROM master2 WHERE alert_code = '$alertCode'"

    val dtCount: Long = 100L // Expected count

    // First, ensure the SQL scripts are executed
    if (!ExecutionTracker.isCompleted(alertCode)) {
      runSqlScript(alertCode) // Run SQL scripts for the alertCode
    }

    val dataLoaded = waitForDataToLoadAndValidate(alertCode, countQuery1, countQuery2, dtCount)

    if (dataLoaded) {
      println(s"Master1 and Master2 data validation passed for alertCode = $alertCode")
    } else {
      println(s"Validation failed for alertCode = $alertCode")
    }
  }
}
