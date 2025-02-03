import scala.util.Try
import scala.concurrent.duration._
import java.lang.Thread.sleep
import java.sql.{Connection, DriverManager}

def waitForDataToLoadAndValidate(
    alertCode: String,
    query: String,
    url: String,
    username: String,
    password: String,
    driver: String,
    maxRetries: Int = 5,  
    initialDelay: FiniteDuration = 2.seconds 
): Boolean = {
    
    var attempt = 0
    var delay = initialDelay.toMillis
    var connection: Connection = null

    try {
        // Load JDBC driver
        Class.forName(driver)

        // Establish a JDBC connection
        connection = DriverManager.getConnection(url, username, password)
        connection.setAutoCommit(false)  // Disable auto-commit to manually commit

        while (attempt < maxRetries) {
            println(s"Attempt ${attempt + 1}: Executing query: $query")

            val countDF = spark.read
                .format("jdbc")
                .option("url", url)
                .option("dbtable", s"($query) AS subquery")
                .option("user", username)
                .option("password", password)
                .option("driver", driver)
                .load()

            val dtCount = Try {
                val row = countDF.collect().headOption
                row.map(_.getAs[Long]("count")).getOrElse(0L)
            }.getOrElse(0L)

            println(s"Data count for alertCode $alertCode: $dtCount")

            if (dtCount > 0) {
                println(s"✅ Data successfully loaded for alertCode: $alertCode.")

                // Commit the transaction since data is found
                connection.commit()
                println(s"✅ Data committed successfully.")

                return true  // Exit early since data is found
            } else {
                println(s"⚠ No data found yet for alertCode $alertCode. Retrying in ${delay / 1000} seconds...")
                sleep(delay) 
                delay *= 2 
            }
            
            attempt += 1
        }

        println(s"❌ Data still not available after $maxRetries retries. Failing process.")
        false

    } catch {
        case e: Exception =>
            println(s"❌ Error occurred: ${e.getMessage}")
            if (connection != null) connection.rollback() // Rollback on failure
            false

    } finally {
        if (connection != null) {
            connection.close()
            println("✅ Connection closed successfully.")
        }
    }
}
