import java.sql.{Connection, ResultSet}

def transformInsertToIncludeColumns(query: String, conn: Connection): String = {
  val insertSelectPattern = """(?i)INSERT INTO\s+(\w+)\s+SELECT\s+(.+?)\s+FROM\s+(\w+)""".r

  query match {
    case insertSelectPattern(tableName, selectColumns, selectTable) =>
      // Fetch the column names for the target table
      val stmt = conn.createStatement()
      val rs: ResultSet = stmt.executeQuery(s"SELECT * FROM $tableName WHERE 1=0") // No data retrieval
      val metaData = rs.getMetaData
      val columnCount = metaData.getColumnCount

      // Extract column names from the metadata
      val columns = (1 to columnCount).map(metaData.getColumnName)

      if (columns.isEmpty) {
        println(s"Warning: No columns found for table $tableName. Returning original query.")
        query
      } else {
        // Transform the query to include column names
        s"INSERT INTO $tableName (${columns.mkString(", ")}) SELECT $selectColumns FROM $selectTable"
      }

    case _ => query // Return the original query if it doesn't match
  }
}


