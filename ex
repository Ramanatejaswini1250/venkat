import org.apache.spark.sql.DataFrame

def executeCountQuery(query: String): Option[Long] = {
  try {
    val countDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"($query) AS subquery")
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("driver", jdbcDriver)
      .load()

    Some(countDF.collect()(0).getLong(0))
  } catch {
    case e: Exception =>
      println(s"Error executing query: ${e.getMessage}")
      None
  }
}

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

    executeCountQuery(countQuery1) match {
      case Some(count1) if count1 == dtCount =>
        println(s"count1 matched dtCount: $count1")

        executeCountQuery(countQuery2) match {
          case Some(count2) if count2 > 0 =>
            println(s"Data loaded successfully: count1 = $count1, count2 = $count2")
            dataLoaded = true
          case _ =>
            println(s"Data for alertCode = $alertCode not available in table2.")
        }
      
      case _ =>
        println(s"count1 did not match dtCount or failed. Retrying in ${retryInterval / 1000} seconds...")
    }

    if (!dataLoaded) {
      retries += 1
      Thread.sleep(retryInterval)
    }
  }

  if (!dataLoaded) {
    println(s"Data load failed for alertCode = $alertCode after $maxRetries retries.")
  }

  dataLoaded
}
