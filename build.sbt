import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{FileOutputStream, BufferedOutputStream}
import java.net.URI

val hdfsPath = "hdfs://nameservice1/tmp/ramp/20250210232051_RBSCC_MON/"
val localPath = "/disk1/bigdata/dev/source/ramp/testing/etl-ramp-automation/run/output"

val fs = FileSystem.get(new URI(hdfsPath), spark.sparkContext.hadoopConfiguration)

try {
  val files = fs.listStatus(new Path(hdfsPath)).map(_.getPath)

  files.foreach { file =>
    val inputStream = fs.open(file)
    val outputFile = new java.io.File(s"$localPath/${file.getName}")
    val outputStream = new BufferedOutputStream(new FileOutputStream(outputFile))

    try {
      val buffer = new Arrayfor reading data
      var bytesRead = inputStream.read(buffer)

      while (bytesRead != -1) {
        outputStream.write(buffer, 0, bytesRead)
        bytesRead = inputStream.read(buffer)
      }

      println(s"Copied file: ${file.getName} to $localPath")
    } finally {
      inputStream.close()
      outputStream.close()
    }
  }
} catch {
  case e: Exception =>
    println(s"Error occurred: ${e.getMessage}")
    e.printStackTrace()
}
