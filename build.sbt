import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkFiles
import java.nio.file.{Files, Paths, StandardCopyOption}

object HDFSFileToLocal {
  def main(args: Array[String]): Unit = {
    // 1️⃣ Initialize Spark Context
    val conf = new SparkConf().setAppName("HDFS File to Local").setMaster("yarn")
    val sc = new SparkContext(conf)

    // 2️⃣ HDFS File Path
    val hdfsFilePath = "hdfs:///tmp/ramp/20251010331122_RBSCCS_TUE"

    // 3️⃣ Add File to Spark Context (Distributes it to worker nodes)
    sc.addFile(hdfsFilePath)

    // 4️⃣ Get Local Path from SparkFiles (this is on the worker node)
    val workerLocalPath = SparkFiles.get("20251010331122_RBSCCS_TUE")

    // 5️⃣ Define Destination Path on Driver Node
    val driverLocalPath = "/home/user/20251010331122_RBSCCS_TUE"

    try {
      // 6️⃣ Move File to Desired Location
      Files.copy(
        Paths.get(workerLocalPath),
        Paths.get(driverLocalPath),
        StandardCopyOption.REPLACE_EXISTING
      )

      println(s"✅ File successfully copied to: $driverLocalPath")
    } catch {
      case e: Exception => println(s"❌ Error copying file: ${e.getMessage}")
    }

    // 7️⃣ Stop Spark Context
    sc.stop()
  }
}
