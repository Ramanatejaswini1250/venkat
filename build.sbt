import java.sql.{Connection, DriverManager, ResultSet, Statement}

// JDBC Setup (Replace with actual values)
val url = "jdbc:your_database_url"
val driver = "your_database_driver"
val username = "your_database_username"
val password = "your_database_password"

// Establish JDBC connection
val connection = DriverManager.getConnection(url, username, password)
val stmt_rss = connection.createStatement()

// SQL query to execute
val alertCode = "SomeAlertCode"  // Replace with actual alert code value
val master1 = s"""SELECT * FROM your_table_name WHERE alertcode='$alertCode' ORDER BY 1"""

// Execute the query
val resultSet: ResultSet = stmt_rss.executeQuery(master1)

// Check if there are records to show
if (resultSet.next()) {
  // Print column names (optional)
  val metaData = resultSet.getMetaData
  val columnCount = metaData.getColumnCount
  for (i <- 1 to columnCount) {
    print(s"${metaData.getColumnName(i)}\t")
  }
  println()

  // Print the rows
  do {
    // Iterate through the result set and print each row
    for (i <- 1 to columnCount) {
      print(s"${resultSet.getString(i)}\t")
    }
    println()  // New line after each row
  } while (resultSet.next())  // Continue while there are more rows
} else {
  println("No records found!")
}

// Close the resources
resultSet.close()
stmt_rss.close()
connection.close()
