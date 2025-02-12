import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ListBuffer

// Function to perform data validation (compare CSV and table data)
def validateData(tableDataPath: String, csvFilePath: String): Boolean = {
  val spark = SparkSession.builder().getOrCreate()
  
  // Load table data and CSV data
  val tableDF = spark.read.format("csv").option("header", "true").load(tableDataPath)
  val csvDF = spark.read.format("csv").option("header", "true").load(csvFilePath)

  // Compare DataFrames (assumes schema and order match)
  tableDF.except(csvDF).isEmpty && csvDF.except(tableDF).isEmpty
}

// Function to copy from HDFS to local
def copyFromHDFSToLocal(hdfsPaths: Seq[String], localOutputDir: String): Seq[String] = {
  val spark = SparkSession.builder().getOrCreate()
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(hadoopConf)

  hdfsPaths.map { path =>
    val hdfsPath = new Path(path)
    val localPath = new Path(localOutputDir, hdfsPath.getName)
    fs.copyToLocalFile(false, hdfsPath, localPath, true) // Overwrites if already exists
    s"file://${localPath.toString}" // Ensure local paths are prefixed with file://
  }
}

// Main Logic
val spark = SparkSession.builder().appName("ConditionalCopyExample").getOrCreate()

// Step 1: Load DataFrame
val df = spark.read.format("parquet").load("/path/to/hdfs/data")

// Step 2: Collect and validate paths inside a single `df.foreachPartition`
val hdfsPathsToCopy = new ListBuffer[String]()

df.foreachPartition { partition =>
  partition.foreach { row =>
    val csvFilePath = "/path/to/local/csv/data.csv"
    val tableDataPath = "/path/to/table/data" // Replace with actual path or query

    // Perform validation
    if (validateData(tableDataPath, csvFilePath)) {
      // Collect paths for copying
      val filePath = row.getAs[String]("file_path_column") // Replace with actual column
      hdfsPathsToCopy.synchronized { hdfsPathsToCopy += filePath }
    }
  }
}

// Step 3: Copy files from HDFS to local after validation
val localOutputDir = "/path/to/local/driver/destination"
val localPaths = copyFromHDFSToLocal(hdfsPathsToCopy.toSeq, localOutputDir)

// Step 4: Broadcast the copied local paths
val broadcastLocalPaths = spark.sparkContext.broadcast(localPaths)

// Step 5: Further processing using broadcasted local paths
df.foreachPartition { partition =>
  partition.foreach { row =>
    val rowPath = "file://" + row.getAs[String]("file_path_column")
    if (broadcastLocalPaths.value.contains(rowPath)) {
      println(s"Validation and copy confirmed for: $rowPath")
    }
  }
}

spark.stop()
