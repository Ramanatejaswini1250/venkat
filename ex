import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

def executeSqlCommand(sqlQuery: String): Boolean = {
  try {
    val countDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"($sqlQuery) AS subquery")
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("driver", jdbcDriver)
      .load()

    countDF.isEmpty match {
      case true =>
        println(s"No data returned from SQL query: $sqlQuery")
        false
      case false =>
        println(s"SQL command executed successfully for: $sqlQuery")
        true
    }
  } catch {
    case e: Exception =>
      println(s"Error executing SQL command: ${e.getMessage}")
      false
  }
}

def waitForSqlExecutionCompletion(sqlQuery: String): Boolean = {
  val promise = Promise[Boolean]()
  
  // Execute the SQL command asynchronously and complete the promise once done
  Future {
    val result = executeSqlCommand(sqlQuery)
    promise.success(result)
  }
  
  // Block until the promise is completed (timeout handling can be added here)
  try {
    Await.result(promise.future, 10.minutes) // Block until the future completes (set an appropriate timeout)
  } catch {
    case e: Exception =>
      println(s"Error waiting for SQL command to complete: ${e.getMessage}")
      false
  }
}

def waitForDataToLoadAndValidate(
    alertCode: String,
    countQuery1: String,
    countQuery2: String,
    dtCount: Long
): Boolean = {
  val sqlExecutionSuccess = waitForSqlExecutionCompletion(countQuery1) && waitForSqlExecutionCompletion(countQuery2)
  
  if (!sqlExecutionSuccess) {
    println(s"SQL execution failed, cannot proceed with validation for alertCode = $alertCode.")
    return false
  }

  println(s"SQL execution completed successfully for alertCode = $alertCode. Proceeding with validation.")
  
  // Execute the rest of the validation logic here
  var retries = 0
  var dataLoaded = false
  while (retries < 10 && !dataLoaded) {
    println(s"Attempt ${retries + 1}: Checking data for alertCode = $alertCode")

    val count1 = executeCountQuery(countQuery1)
    println(s"count1 retrieved: $count1, Expected dtCount: $dtCount")

    if (count1 == dtCount) {
      val count2 = executeCountQuery(countQuery2)
      println(s"count2 retrieved: $count2")

      if (count2 > 0) {
        dataLoaded = true
        println(s"Data loaded successfully for alertCode = $alertCode")
      } else {
        println(s"Data not available in table2 for alertCode = $alertCode.")
      }
    }

    if (!dataLoaded) {
      retries += 1
      println(s"Retrying in 10 seconds...")
      Thread.sleep(10000)
    }
  }

  if (!dataLoaded) {
    println(s"Data load failed for alertCode = $alertCode after retries.")
  }

  dataLoaded
}

// Now, calling this in the `df.foreachPartition` for master1 and master2 logic
df.foreachPartition { partition =>
  partition.foreach { row =>
    val alertCode = row.getAs[String]("alertCode")
    
    // Example queries for master1 and master2
    val countQuery1 = "SELECT COUNT(*) FROM master1 WHERE alert_code = '" + alertCode + "'"
    val countQuery2 = "SELECT COUNT(*) FROM master2 WHERE alert_code = '" + alertCode + "'"
    
    // Ensure the SQL queries are completed before proceeding with validation
    val dtCount: Long = 100L  // Example expected count

    val dataLoaded = waitForDataToLoadAndValidate(alertCode, countQuery1, countQuery2, dtCount)
    
    if (dataLoaded) {
      println(s"Master1 and Master2 data validation passed for alertCode = $alertCode")
      // Proceed with the further logic if needed
    } else {
      println(s"Validation failed for alertCode = $alertCode")
    }
  }
}
