import java.text.SimpleDateFormat
import java.util.Date
import scala.sys.process._

val timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
val hdfsPath = "hdfs://nameservice1/tmp/ramp/20250211224800_RBSCC_TUE"
val localBasePath = "/disk1/bigdata/dev/source/ramp/hdfs_output"
val localPath = s"$localBasePath/hdfs_copy_$timestamp"

// Create the local directory before copying
val mkdirCommand = s"mkdir -p $localPath"
mkdirCommand.!

// Execute the copy command
val copyCommand = s"hadoop fs -copyToLocal $hdfsPath $localPath"
println(s"Running command: $copyCommand")

val result = copyCommand.!

if (result == 0) {
  println(s"✅ Successfully copied $hdfsPath to $localPath")
} else {
  println(s"❌ Failed to copy $hdfsPath to $localPath")
}
