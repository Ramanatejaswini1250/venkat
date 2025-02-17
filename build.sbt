def transformInsertToIncludeColumns(query: String): String = {
  val insertSelectPattern = """(?i)INSERT INTO\s+(\w+)\s*\((.*?)\)\s*SELECT\s+(.+)""".r
  query match {
    case insertSelectPattern(tableName, columns, selectStatement) =>
      s"INSERT INTO $tableName ($columns) SELECT $columns FROM ($selectStatement)"
    case _ => query // Return original query if it doesn't match
  }
}
