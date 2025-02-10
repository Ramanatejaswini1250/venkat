import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedOutputStream, FileOutputStream}
import java.net.URI

val hdfsSrc = new Path("hdfs:///tmp/venkat/current_timestamp_rbscc_mon") // No trailing "/"
val localDst = new File("/disk1/ramp/output/")

val fs = FileSystem.get(new URI("hdfs:///"), spark.sparkContext.hadoopConfiguration)

if (fs.exists(hdfsSrc) && fs.getFileStatus(hdfsSrc).isDirectory) {  
  val finalLocalPath = new File(localDst, hdfsSrc.getName)
  
  if (!finalLocalPath.exists()) {
    finalLocalPath.mkdirs() // ✅ Creates the directory
  }

  // ✅ Copy all contents recursively
  def copyRecursively(hdfsPath: Path, localPath: File)(implicit fs: FileSystem): Unit = {
    val fileStatuses = fs.listStatus(hdfsPath)

    for (fileStatus <- fileStatuses) {
      val hdfsFile = fileStatus.getPath
      val localFile = new File(localPath, hdfsFile.getName)

      if (fileStatus.isDirectory) {
        localFile.mkdirs()
        copyRecursively(hdfsFile, localFile) // Recursive copy for subdirectories
      } else {
        val inputStream = fs.open(hdfsFile)
        val outputStream = new BufferedOutputStream(new FileOutputStream(localFile))

        try {
          val buffer = new Array[Byte](4 * 1024)
          var bytesRead = inputStream.read(buffer)

          while (bytesRead != -1) {
            outputStream.write(buffer, 0, bytesRead)
            bytesRead = inputStream.read(buffer)
          }
        } finally {
          inputStream.close()
          outputStream.close()
        }
      }
    }
  }

  copyRecursively(hdfsSrc, finalLocalPath)(fs)
  println(s"✅ Successfully moved folder: $hdfsSrc → $finalLocalPath")
} else {
  println(s"🚨 ERROR: $hdfsSrc is NOT a directory or does not exist!")
}
