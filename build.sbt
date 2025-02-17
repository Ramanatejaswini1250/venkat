import java.sql.{Connection, ResultSet}

def transformInsertToIncludeColumns(query: String, conn: Connection): String = {
  // Define the regex pattern to match the "INSERT INTO table SELECT ..." structure
  val insertSelectPattern = """(?i)INSERT INTO\s+(\w+)\s+(SELECT.*)""".r

  query match {
    case insertSelectPattern(tableName, selectClause) =>
      // Inner function to fetch columns for the target table
      def getTableColumns(tableName: String, conn: Connection): Seq[String] = {
        val query = s"SELECT * FROM $tableName WHERE 1=0" // Dummy query to get metadata
        val stmt = conn.createStatement()
        val rs: ResultSet = stmt.executeQuery(query)
        val metaData = rs.getMetaData
        (1 to metaData.getColumnCount).map(metaData.getColumnName)
      }

      // Fetch all column names for the target table
      val columns = getTableColumns(tableName, conn)
      
      if (columns.isEmpty) {
        println(s"Warning: No columns found for table $tableName. Returning original query.")
        query
      } else {
        // Transform the query to explicitly include column names
        s"INSERT INTO $tableName (${columns.mkString(", ")}) $selectClause"
      }

    case _ =>
      // Return the original query if no match
      query
  }
}
