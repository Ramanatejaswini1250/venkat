import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ListBuffer

object RampAutomationExecution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Ramp Automation Execution").getOrCreate()

    // Function to copy from HDFS to Local
    def copyFromHDFSToLocal(hdfsPaths: Seq[String], localOutputDir: String): Seq[String] = {
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = FileSystem.get(hadoopConf)

      val localPaths = hdfsPaths.map { hdfsFilePath =>
        val hdfsPath = new Path(hdfsFilePath)
        val localPath = new Path(localOutputDir, hdfsPath.getName)
        fs.copyToLocalFile(false, hdfsPath, localPath, true) // Overwrites if already exists
        s"file://${localPath.toString}" // Ensure local paths are prefixed with file://
      }

      localPaths
    }

    // Initialize collection to hold HDFS paths for copying
    val hdfsPathsToCopy = new ListBuffer[String]()
    val localOutputDir = "/disk1/bigdata/dev/source/ramp"

    // Step 1: Iterate through DataFrame rows to check validation
    df.foreachPartition { partition =>
      partition.foreach { row =>
        val Master_FILE_SUCCESS_Query = "YOUR SQL QUERY HERE"
        stmt.executeQuery(Master_FILE_SUCCESS_Query)

        println("Starting data validation between Master Table 2 and exported Master File 2 CSV...")

        if (dataConsistent2) { // Assume dataConsistent2 is a boolean flag indicating validation success
          println("Data validation successful: Master Table 2 matches Master File 2 CSV.")

          // Add HDFS path for copying
          val filePath = row.getAs[String]("hdfs_file_column") // Replace with actual column name
          hdfsPathsToCopy.synchronized { hdfsPathsToCopy += filePath }
        }
      }
    }

    // Step 2: Copy validated HDFS paths to local
    val localPaths: Seq[String] = copyFromHDFSToLocal(hdfsPathsToCopy.toSeq, localOutputDir)

    // Step 3: Broadcast the local paths
    val broadcastLocalPaths = spark.sparkContext.broadcast(localPaths)

    // Step 4: Verify broadcasted paths
    df.foreachPartition { partition =>
      partition.foreach { row =>
        val rowPath = s"file://${row.getAs[String]("hdfs_file_column")}" // Ensure the "file://" prefix
        if (broadcastLocalPaths.value.contains(rowPath)) {
          println(s"Validation and copy confirmed for: $rowPath")
        }
      }
    }

    spark.stop()
  }
}
