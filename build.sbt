import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import sys.process._

val spark = SparkSession.builder.appName("ConditionalCopy").getOrCreate()

// Step 1: Load your DataFrame
val df = spark.read.format("parquet").load("hdfs://path/to/RAMP_MASTER_TARGET1")

// Step 2: Create an accumulator for validation status
val validationSuccessAccumulator: LongAccumulator = spark.sparkContext.longAccumulator("ValidationSuccess")

// Step 3: Perform validation inside df.foreachPartition
df.foreachPartition { partition =>
  var partitionValidationSuccess = true

  partition.foreach { row =>
    // Your validation logic here
    // If any row fails validation, set partitionValidationSuccess to false
    if (/* validation fails for row */ false) {
      partitionValidationSuccess = false
    }
  }

  // Update the accumulator only if validation fails
  if (!partitionValidationSuccess) {
    validationSuccessAccumulator.add(1)
  }
}

// Step 4: Check the accumulator value in the driver
if (validationSuccessAccumulator.value == 0) {
  println("Validation passed for all partitions. Proceeding to copy data.")

  // Step 5: Copy the data from HDFS to the driver's local location
  val hdfsPath = "hdfs://path/to/processed_output"
  val localPath = "/driver/location/processed_output"
  
  // Save to HDFS first
  df.write.mode("overwrite").csv(hdfsPath)

  // Copy to the driver's local location
  s"hadoop fs -copyToLocal $hdfsPath $localPath".!

  println(s"Data successfully copied to $localPath")
} else {
  println("Validation failed. Skipping data copy.")
}
