// package example

// object Hello extends Greeting with App {
//   println(greeting)
// }

// trait Greeting {
//   lazy val greeting: String = "hello"
// }

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File

object Main {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("ComicAnalysis")
      .master("local[*]") // You can change this to your Spark cluster URL
      .getOrCreate()

    // Load text file into a DataFrame
    val textDF = spark.read.text("data/comics.txt")

    // Extract comic names and count occurrences
    import spark.implicits._
    val comicDF = textDF
      .flatMap(_.getString(0).split("-").lift(1))
      .withColumn("comicName", trim(split(col("value"), "-").getItem(0)))
      .groupBy("comicName")
      .count()
      .orderBy(desc("count"))

    // Show the result
    comicDF.show(truncate = false)

    // Output directory and CSV file path
    val outputDir = "output"
    val csvFilePath = s"$outputDir"

    // Check if the file already exists and delete it if it does
    val outputFile = new File(outputDir)
    if (outputFile.exists()) {
      outputFile.delete()
    }

    // Write the result to a CSV file
    comicDF
      .coalesce(1) // Coalesce to a single partition to write to a single CSV file
      .write
      .mode("overwrite") // Overwrite mode
      .option("header", "true")
      .csv(outputDir)

    // Stop SparkSession
    spark.stop()
  }
}
