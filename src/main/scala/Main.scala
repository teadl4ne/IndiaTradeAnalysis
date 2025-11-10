import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.PropertyConfigurator

object Main {

  def main(args: Array[String]): Unit = {

    // Configuring logging to suppress INFO/DEBUG spam.
    PropertyConfigurator.configure("src/main/resources/log4j.properties")

    // Initializing SparkSession with explicit local master and host binding
    val spark = SparkSession.builder()
      .appName("IndiaTradeAnalysis")
      .master("local[*]") // Explicitly defining local mode
      .config("spark.driver.bindAddress", "127.0.0.1") // Forcing loopback address to fix Invalid Spark URL
      .config("spark.driver.host", "127.0.0.1")        // Preventing host name resolution errors
      .config("spark.network.timeout", "600s")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("SparkSession initialized without errors!")
    println(s"Spark master: ${spark.sparkContext.master}")
    println(s"Spark version: ${spark.version}")

    // Path to input CSV
    val csvPath = "src/data/exports_and_imports_of_india.csv"

    // TASK 1–2: Read dataset
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)
      .cache()

    println(s"Loaded ${df.count()} rows and ${df.columns.length} columns.")

    // TASK 3: Header
    println("\nTASK 3 — Header:")
    println(df.columns.mkString(", "))

    // TASK 4: First 5 rows
    println("\nTASK 4 — First 5 rows:")
    df.show(5, truncate = false)

    // TASK 5: Sort descending by Country
    println("\nTASK 5 — Sorted by Country (desc):")
    import spark.implicits._
    val byCountryDesc = df.orderBy($"Country".desc)
    byCountryDesc.show(5, truncate = false)

    // TASK 6: Drop Financial Year(end)
    println("\nTASK 6 — Drop column 'Financial Year(end)':")
    val noYearEnd = byCountryDesc.drop("Financial Year(end)")
    noYearEnd.show(5, truncate = false)

    // TASK 7: Sort by highest Export
    println("\nTASK 7 — Sorted by Export (desc):")
    val byExportDesc = noYearEnd.orderBy($"Export".desc)
    byExportDesc.show(5, truncate = false)

    // TASK 8: Add mean column (Total Trade & Trade Balance)
    println("\nTASK 8 — Add 'Mean_Total' column (mean of Total Trade and Trade Balance):")
    val cleaned = cleanNumericColumns(byExportDesc, Seq("Export", "Import", "Total Trade", "Trade Balance"))
    val withMean = cleaned.withColumn("Mean_Total", (col("Total Trade") + col("Trade Balance")) / 2)
    withMean.show(5, truncate = false)
    withMean.printSchema()

    // TASK 9: Function to get the lowest value by column index
    println("\nTASK 9 — Lowest value by column index:")
    val idx = 3 // example: zero-based index; change as needed
    getLowestValue(withMean, idx, spark) match {
      case Some(v) => println(s"Lowest value in column #$idx (${withMean.columns.lift(idx).getOrElse("N/A")}): $v")
      case None    => println(s"Invalid column index: $idx")
    }

    // TASK 10: Save DataFrame to CSV
    println("\nTASK 10 — Write processed DataFrame to CSV:")
    val outDir = "src/data/output/india_trade_processed"
    withMean.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outDir)
    println(s"Data written to: $outDir (note: Spark writes part-files and _SUCCESS)")

    // Clean up
    spark.stop()
  }

  // Replace thousands separators (commas) and cast to double for a set of columns
  private def cleanNumericColumns(df: DataFrame, cols: Seq[String]): DataFrame =
    cols.foldLeft(df) { (acc, c) =>
      if (acc.columns.contains(c))
        acc.withColumn(c, regexp_replace(col(c), ",", "").cast("double"))
      else acc
    }

  // Get the lowest value from a dataframe column by zero-based index
  private def getLowestValue(df: DataFrame, columnIndex: Int, spark: SparkSession): Option[Double] = {
    import spark.implicits._
    df.columns.lift(columnIndex) match {
      case Some(name) =>
        val row = df.select(min(col(name).cast("double"))).as[Double].collect().headOption
        row
      case None =>
        None
    }
  }
}
