import org.apache.hadoop.fs.{FileSystem, Path, FileStatus, FSDataInputStream}
import java.io.{BufferedOutputStream, FileOutputStream, File}
import java.net.URI

val hdfsFolderPath = "hdfs://nameservice1/tmp/venkat/current_timestamp_rbscc_mon/"
val localFolderPath = "/disk1/ramp/output/current_timestamp_rbscc_mon/"

// Get HDFS FileSystem instance
val fs = FileSystem.get(new URI(hdfsFolderPath), spark.sparkContext.hadoopConfiguration)

try {
  val hdfsPath = new Path(hdfsFolderPath)
  val localPath = new File(localFolderPath)

  if (!localPath.exists()) {
    localPath.mkdirs() // Create root folder in local
  }

  if (fs.exists(hdfsPath) && fs.isDirectory(hdfsPath)) {
    // ✅ Recursively list all files and folders
    def copyRecursively(hdfsSrc: Path, localDst: File): Unit = {
      val fileStatuses: Array[FileStatus] = fs.listStatus(hdfsSrc)

      for (fileStatus <- fileStatuses) {
        val hdfsFilePath = fileStatus.getPath
        val localFilePath = new File(localDst, hdfsFilePath.getName)

        if (fileStatus.isDirectory) {
          // ✅ Create local subdirectory
          if (!localFilePath.exists()) {
            localFilePath.mkdirs()
          }
          // ✅ Recursively copy subdirectory
          copyRecursively(hdfsFilePath, localFilePath)
        } else {
          // ✅ Copy individual file
          val inputStream: FSDataInputStream = fs.open(hdfsFilePath)
          val outputStream = new BufferedOutputStream(new FileOutputStream(localFilePath))

          try {
            val buffer = new Array[Byte](4 * 1024) // 4KB buffer
            var bytesRead = inputStream.read(buffer)

            while (bytesRead != -1) {
              outputStream.write(buffer, 0, bytesRead)
              bytesRead = inputStream.read(buffer)
            }

            println(s"Copied file: $hdfsFilePath -> $localFilePath")
          } finally {
            inputStream.close()
            outputStream.close()
          }
        }
      }
    }

    // ✅ Start recursive copy
    copyRecursively(hdfsPath, localPath)

    // ✅ Verify all files copied bef
