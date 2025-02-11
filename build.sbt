import java.text.SimpleDateFormat
import java.util.Date
import scala.sys.process._

object HdfsToLocalCopy {
  def main(args: Array[String]): Unit = {
    // Generate a dynamic timestamp
    val timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())

    // Define HDFS source path
    val hdfsPath = "hdfs://nameservice1/tmp/ramp/20250211224800_RBSCC_TUE"

    // Define the local destination path dynamically
    val localBasePath = "/disk1/bigdata/dev/source/ramp/hdfs_output"
    val localPath = s"$localBasePath/hdfs_copy_$timestamp"

    // Step 1: Create the local directory
    val mkdirCommand = s"mkdir -p $localPath"
    val mkdirExitCode = mkdirCommand.!

    if (mkdirExitCode == 0) {
      println(s"✅ Created directory: $localPath")
    } else {
      println(s"❌ Failed to create directory: $localPath")
      System.exit(1) // Exit if directory creation fails
    }

    // Step 2: Copy files from HDFS to local
    val copyCommand = s"hadoop fs -copyToLocal $hdfsPath $localPath"
    val copyExitCode = copyCommand.!

    if (copyExitCode == 0) {
      println(s"✅ Successfully copied $hdfsPath to $localPath")
    } else {
      println(s"❌ Failed to copy $hdfsPath to $localPath")
    }
  }
}
