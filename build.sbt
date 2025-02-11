import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkFiles
import org.apache.hadoop.fs.{FileUtil, FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.io.File

object HDFSFileToLocal {
  def main(args: Array[String]): Unit = {
    // 1️⃣ Initialize Spark Context
    val conf = new SparkConf().setAppName("HDFS File to Local").setMaster("yarn") // Adjust master as needed
    val sc = new SparkContext(conf)

    // 2️⃣ HDFS File Path
    val hdfsFilePath = "hdfs:///tmp/ramp/20251010331122_RBSCCS_TUE"

    // 3️⃣ Add File to Spark Context
    sc.addFile(hdfsFilePath)

    // 4️⃣ Get the Local File Path from SparkFiles (on worker node)
    val localFilePath = SparkFiles.get("20251010331122_RBSCCS_TUE")

    // 5️⃣ Define Destination Path in Local Filesystem
    val localDestinationPath = "/home/user/20251010331122_RBSCCS_TUE"

    try {
      // 6️⃣ Copy File to Local Using FileUtil.copy()
      val hadoopConf = new Configuration()
      val fs = FileSystem.get(hadoopConf)
      val hdfsPath = new Path(localFilePath)  // Source (HDFS path on worker node)
      val localPath = new Path(localDestinationPath) // Destination (Local path)

      // Perform copy operation
      val copied = FileUtil.copy(fs, hdfsPath, localPath, false, hadoopConf)

     
