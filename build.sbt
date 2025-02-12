import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.io.File

object HDFSDirectoryCopy {
  def main(args: Array[String]): Unit = {
    // 1️⃣ HDFS Configuration
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)

    // 2️⃣ Define HDFS Source and Local Destination
    val hdfsPath = new Path("hdfs:///tmp/ramp/20251010331122_RBSCCS_TUE")
    val localPath = new Path("/disk/ramp/20251010331122_RBSCCS_TUE")

    // 3️⃣ Create the local directory if it doesn't exist
    val localDir = new File(localPath.toString)
    if (!localDir.exists()) {
      localDir.mkdirs() // Create the directory
      println(s"Created local directory: $localPath")
    }

    // 4️⃣ Copy from HDFS to Local
    fs.copyToLocalFile(false, hdfsPath, localPath, true)
    println(s"Copied directory from HDFS ($hdfsPath) to Local ($localPath)")

    // 5️⃣ Verify the files in the copied directory
    val copiedFiles = localDir.listFiles()
    if (copiedFiles != null && copiedFiles.nonEmpty) {
      println(s"Files copied to $localPath:")
      copiedFiles.foreach(file => println(s"- ${file.getName}"))
    } else {
      println(s"No files found in the copied directory: $localPath")
    }
  }
}
