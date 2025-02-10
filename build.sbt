import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, FSDataInputStream}
import java.io.{BufferedOutputStream, FileOutputStream}
import java.net.URI

val hdfsFolderPath = "hdfs://nameservice1/tmp/venkat/current_timestamp_rbscc_mon/"
val localFolderPath = "/disk1/ramp/output/"

// Get HDFS FileSystem instance
val fs = FileSystem.get(new URI(hdfsFolderPath), spark.sparkContext.hadoopConfiguration)

try {
  val hdfsPath = new Path(hdfsFolderPath)
  val localPath = new java.io.File(localFolderPath)

  if (!localPath.exists()) {
    localPath.mkdirs() // Create local directory if not exists
  }

  if (fs.exists(hdfsPath) && fs.isDirectory(hdfsPath)) {
    // ✅ List all `.csv` files in HDFS folder
    val hdfsFiles: Array[FileStatus] = fs.listStatus(hdfsPath).filter(_.getPath.getName.endsWith(".csv"))

    for (fileStatus <- hdfsFiles) {
      val hdfsFilePath = fileStatus.getPath
      val localFilePath = new java.io.File(localFolderPath, hdfsFilePath.getName)

      // ✅ Read file from HDFS
      val inputStream: FSDataInputStream = fs.open(hdfsFilePath)
      val outputStream = new BufferedOutputStream(new FileOutputStream(localFilePath))

      try {
        val buffer = new Array[Byte](4 * 1024) // 4KB buffer
        var bytesRead = inputStream.read(buffer)

        while (bytesRead != -1) { // Read until EOF
          outputStream.write(buffer, 0, bytesRead) // Write to local file
          bytesRead = inputStream.read(buffer)
        }

        println(s"Copied file: $hdfsFilePath -> $localFilePath")
      } finally {
        inputStream.close()
        outputStream.close()
      }
    }

    // ✅ Verify all `.csv` files are copied successfully
    val localFiles = localPath.listFiles().filter(_.getName.endsWith(".csv"))
    if (localFiles.length == hdfsFiles.length) {
      println("All CSV files copied successfully.")

      // ✅ Delete the HDFS folder after successful copy
      fs.delete(hdfsPath, true)
      println(s"Deleted HDFS folder: $hdfsFolderPath")
    } else {
      println("File count mismatch. Not deleting HDFS folder.")
    }
  } else {
    println(s"HDFS path does not exist or is not a directory: $hdfsFolderPath")
  }
} catch {
  case e: Exception =>
    println(s"Error occurred: ${e.getMessage}")
    e.printStackTrace()
}
