import java.sql.{Connection, DriverManager, Statement}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.util.{Try, Using}
import scala.collection.mutable.ListBuffer

def runSqlScript(
    scriptPath: String,
    jdbcUrl: String,
    jdbcUser: String,
    jdbcPassword: String,
    jdbcDriver: String
): Unit = {

  println(s"Running SQL script from: $scriptPath")
  val errorLogs = ListBuffer[String]()

  // Load SQL script content
  val scriptContent = Try(new String(Files.readAllBytes(Paths.get(scriptPath)), StandardCharsets.UTF_8)).getOrElse {
    println(s"[ERROR] Failed to read SQL script: $scriptPath")
    return
  }

  // Remove block comments
  val scriptWithoutComments = removeBlockComments(scriptContent)
  val lines = scriptWithoutComments.split("\n").toSeq

  // Prepare valid SQL lines
  var validLines = lines
    .drop(4) // Skip first 4 lines
    .dropRight(3) // Skip last 3 lines
    .map(removeInlineComments)
    .filter(_.nonEmpty)

  // Define required DELETE statements
  val deleteStatements = List(
    "DELETE FROM master1_test;",
    "DELETE FROM master2_test;"
  )

  // Identify missing DELETE statements
  val missingDeletes = deleteStatements.filterNot { deleteStmt =>
    validLines.exists(_.toUpperCase.contains(deleteStmt.toUpperCase.replace(";", "")))
  }

  // Execute DELETE statements first
  val executionOrder = missingDeletes ++ validLines

  val commands = executionOrder.mkString("\n").split(";").map(_.trim).filter(_.nonEmpty)

  if (commands.isEmpty) {
    println("[WARNING] No valid SQL commands found to execute.")
    return
  }

  // Register JDBC driver and establish connection
  Try(Class.forName(jdbcDriver)).getOrElse {
    println(s"[ERROR] JDBC Driver not found: $jdbcDriver")
    return
  }

  Using.Manager { use =>
    val connection = use(DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword))
    connection.setAutoCommit(false)
    val statement = use(connection.createStatement())

    try {
      for (command <- commands) {
        try {
          println(s"Executing SQL: $command")
          statement.execute(command)
        } catch {
          case ex: Exception =>
            val tableName = extractTableName(command)
            val errorMsg = s"[ERROR] Failed to execute SQL on table: $tableName\nMessage: ${ex.getMessage}"
            println(errorMsg)
            errorLogs.append(errorMsg)
            throw ex // Stop execution and rollback on first failure
        }
      }
      
      connection.commit()
      println("✅ SQL script executed successfully.")
      
    } catch {
      case _: Exception =>
        println("[CRITICAL] Rolling back all changes due to error.")
        connection.rollback()
        errorLogs.append("[CRITICAL] Transaction rolled back due to execution failure.")
    }

  }.recover {
    case ex: Exception =>
      val errorMsg = s"[CRITICAL ERROR] Script execution failed: ${ex.getMessage}"
      println(errorMsg)
      errorLogs.append(errorMsg)
  }

  // Send email notification if errors occurred
  if (errorLogs.nonEmpty) {
    sendEmailNotification("SQL_EXECUTION_ERROR", errorLogs.mkString("\n"), "email@example.com", "BusinessName")
  }
}
