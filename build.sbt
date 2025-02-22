import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

object MasterCsvWriter {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark: SparkSession = SparkSession.builder()
      .appName("Write Master CSV in foreachPartition")
      .getOrCreate()

    // Sample DataFrame with alert data
    val df: DataFrame = spark.createDataFrame(Seq(
      ("ALERT001", 100, "2025-02-22"),
      ("ALERT002", 200, "2025-02-22"),
      ("ALERT003", 300, "2025-02-22")
    )).toDF("alert_code", "priority", "event_timestamp")

    val baseOutputPath = "/disk1/ramp/output"
    val masterCsvPath = new Path(s"$baseOutputPath/master1.csv")

    // ✅ Step 1: Write each partition to a temp file
    df.foreachPartition { partition =>
      val conf = new Configuration()
      val fs = FileSystem.get(conf)

      // Generate a unique temp file per partition
      val tempFilePath = new Path(s"$baseOutputPath/tmp_master1_${System.nanoTime()}.csv")
      val outputStream = fs.create(tempFilePath)
      val writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))

      try {
        partition.foreach { row =>
          val alertCode = row.getString(0)
          val priority = row.getInt(1)
          val eventTimestamp = row.getString(2)

          writer.write(s"$alertCode,$priority,$eventTimestamp\n")
        }
        writer.flush()
      } finally {
        writer.close()
      }
    }

    println("Temporary files created successfully!")

    // ✅ Step 2: Merge all temp files into master1.csv
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val tempFiles = fs.globStatus(new Path(s"$baseOutputPath/tmp_master1_*.csv")).map(_.getPath)

    val outputStream = if (fs.exists(masterCsvPath)) {
      fs.append(masterCsvPath) // Append to master1.csv if it exists
    } else {
      fs.create(masterCsvPath) // Create new master1.csv if not exists
    }

    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"))

    try {
      tempFiles.foreach { tempFile =>
        val tempInputStream = fs.open(tempFile)
        val reader = scala.io.Source.fromInputStream(tempInputStream)
        reader.getLines().foreach(writer.write)
        writer.write("\n") // Ensure each entry is on a new line
        reader.close()
        tempInputStream.close()
      }
      writer.flush()
    } finally {
      writer.close()
    }

    // ✅ Step 3: Clean up temp files
    tempFiles.foreach(fs.delete(_, false))

    println("master1.csv updated successfully!")

    spark.stop()
  }
}
