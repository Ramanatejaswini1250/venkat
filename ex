val master1CountQuery =
  s"SELECT COUNT(*) FROM U_D_DSV_001_RSS_0.RAMP_MASTER_TARGET1_TEST WHERE alertCode = '$alertCode'"

val master2CountQuery =
  s"SELECT COUNT(*) FROM U_D_DSV_001_RSS_0.RAMP_MASTER_TARGET2_TEST WHERE alertCode = '$alertCode'"

// Execute these queries separately
val master1Count = spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", s"($master1CountQuery) AS subquery")
  .option("user", jdbcUser)
  .option("password", jdbcPassword)
  .option("driver", jdbcDriver)
  .load()
  .collect()
  .headOption
  .flatMap(row => Option(row.getLong(0)))
  .getOrElse(0L)

val master2Count = spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", s"($master2CountQuery) AS subquery")
  .option("user", jdbcUser)
  .option("password", jdbcPassword)
  .option("driver", jdbcDriver)
  .load()
  .collect()
  .headOption
  .flatMap(row => Option(row.getLong(0)))
  .getOrElse(0L)
