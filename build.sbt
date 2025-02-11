import java.io.File

val hdfsPath = "hdfs://nameservice1/tmp/ramp/20250211224800_RBSCC_TUE"
val localPath = "/disk1/bigdata/dev/source/ramp/hdfs_output/hdfs_copy_20250211224800"

val process = new ProcessBuilder("hadoop", "fs", "-copyToLocal", hdfsPath, localPath)
  .directory(new File("/"))
  .redirectErrorStream(true)
  .start()

val exitCode = process.waitFor()

if (exitCode == 0) {
  println(s"✅ Successfully copied $hdfsPath to $localPath")
} else {
  println(s"❌ Failed to copy $hdfsPath to $localPath")
}
