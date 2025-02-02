def waitForDataToLoadAndValidate(
    alertCode: String,
    countQuery1: String,
    countQuery2: String,
    dtCount: Long,
    maxRetries: Int = 10,
    retryInterval: Long = 10000
): Boolean = {
  var retries = 0
  var dataLoaded = false

  while (retries < maxRetries && !dataLoaded) {
    println(s"Attempt ${retries + 1}: Checking data for alertCode = $alertCode")

    // Execute countQuery1 and validate its result
    val count1 = executeCountQuery(countQuery1)
    if (count1 == dtCount) {
      println(s"Count1 matches dtCount for alertCode = $alertCode with count1 = $count1.")
      
      // Execute countQuery2 and validate its result only if count1 is correct
      val count2 = executeCountQuery(countQuery2)
      if (count2 > 0) {
        println(s"Data loaded successfully for alertCode = $alertCode with count1 = $count1 and count2 = $count2.")
        dataLoaded = true
      } else {
        println(s"Data for alertCode = $alertCode not available in table2 (count2 <= 0).")
      }
    } else {
      println(s"Count1 does not match dtCount for alertCode = $alertCode (count1 = $count1, dtCount = $dtCount).")
    }

    if (!dataLoaded) {
      retries += 1
      println(s"Retrying in ${retryInterval / 1000} seconds...")
      Thread.sleep(retryInterval)
    }
  }

  if (!dataLoaded) {
    println(s"Data load failed for alertCode = $alertCode after $maxRetries retries.")
  }

  dataLoaded
}

def executeCountQuery(query: String): Long = {
  try {
    val countDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"($query) AS subquery")
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("driver", jdbcDriver)
      .load()

    val count = countDF.collect()(0).getLong(0)
    println(s"Count retrieved: $count")
    count
  } catch {
    case e: Exception =>
      println(s"Error executing query: ${e.getMessage}")
      0L  // Return 0 in case of failure, which can be handled by the caller
  }
}
