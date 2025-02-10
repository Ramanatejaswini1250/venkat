import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File

val hdfsDirPath = "hdfs://your-cluster/path/to/hdfs/files"
val local_Location = "/relative/or/absolute/path/to/local/directory"

// Get the absolute path for the local directory
val absoluteLocalPath = new java.io.File(local_Location).getAbsolutePath

// Print the absolute local path for verification
println(s"Absolute Local Path: $absoluteLocalPath")

// Ensure the local directory exists
new java.io.File(absoluteLocalPath).mkdirs()

// Initialize Hadoop FileSystem
val fs = FileSystem.get(new java.net.URI(hdfsDirPath), SparkContext.getOrCreate().hadoopConfiguration)

try {
    // Copy files from HDFS to the local absolute path
    fs.copyToLocalFile(false, new Path(hdfsDirPath), new Path(absoluteLocalPath), true)
    println(s"Files copied successfully from HDFS to local path: $absoluteLocalPath")

    // Verify the copied files
    val localDir = new File(absoluteLocalPath)
    if (localDir.exists && localDir.isDirectory) {
        val files = localDir.listFiles
        if (files != null && files.nonEmpty) {
            println(s"Copied Files in $absoluteLocalPath:")
            files.foreach(file => println(file.getName))
        } else {
            println(s"No files found in $absoluteLocalPath")
        }
    } else {
        println(s"Local directory does not exist: $absoluteLocalPath")
    }
} catch {
    case e: Exception =>
        println(s"Error occurred: ${e.getMessage}")
}

val copiedFiles = localDir.listFiles
if (copiedFiles != null && copiedFiles.nonEmpty) {
    println(s"Copied files in $absoluteLocalPath:")
    copiedFiles.foreach(file => println(file.getName))
} else {
    println(s"No files found in $absoluteLocalPath")
}

