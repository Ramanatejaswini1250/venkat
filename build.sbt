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

      val localPaths = hdfsPaths.flatMap { hdfsFilePath =>
        val hdfsPath = new Path(hdfsFilePath)
        val localPath = new Path(localOutputDir, hdfsPath.getName)

        if (fs.exists(hdfsPath)) { // Check if the folder or file exists in HDFS
          if (fs.isDirectory(hdfsPath)) {
            fs.copyToLocalFile(false, hdfsPath, localPath, true) // Copy the entire folder
          } else {
            fs.copyToLocalFile(false, hdfsPath, localPath, true) // Copy individual file
          }
          Some(s"file://${localPath.toString}") // Ensure local paths are prefixed with file://
        } else {
          println(s"Path not found in HDFS: $hdfsPath")
          None
        }
      }

      localPaths
    }

    // Initialize HDFS paths to copy
    val hdfsPathsToCopy = new ListBuffer[String]()
    val localOutputDir = "/disk1/bigdata/dev/source/ramp"

    // Step 1: Folder name and HDFS path
    val folderName = "Current_timestamp_RBCSS_WED"
    val hdfsFolderPath = s"hdfs://namespace/tmp/ramp/$folderName"
    hdfsPathsToCopy += hdfsFolderPath

    // Step 2: Copy validated HDFS paths to local
    val localPaths: Seq[String] = copyFromHDFSToLocal(hdfsPathsToCopy.toSeq, localOutputDir)

    // Step 3: Broadcast the local paths
    val broadcastLocalPaths = spark.sparkContext.broadcast(localPaths)

    // Step 4: Check if the copied folder matches folderName and give a success message
    df.foreachPartition { partition =>
      partition.foreach { row =>
        val rowPath = s"file://${localOutputDir}/$folderName" // Use folderName to construct the rowPath

        if (broadcastLocalPaths.value.contains(rowPath)) {
          println(s"Validation and copy confirmed for folder: $rowPath")
        } else {
          println(s"Folder not found or not copied: $rowPath")
        }
      }
    }

    spark.stop()
  }
}
