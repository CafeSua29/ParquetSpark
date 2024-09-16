import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main extends App {
    val spark = SparkSession.builder()
        .appName("ParquetSpark")
        .getOrCreate()

        import spark.implicits._

    val schema = StructType(Array(
        StructField("timeCreate", TimestampType, true),
            StructField("cookieCreate", TimestampType, true),
            StructField("browserCode", IntegerType, true),
            StructField("browserVer", StringType, true),
            StructField("osCode", IntegerType, true),
            StructField("osVer", StringType, true),
            StructField("ip", LongType, true),
            StructField("locId", IntegerType, true),
            StructField("domain", StringType, true),
            StructField("siteId", IntegerType, true),
            StructField("cId", IntegerType, true),
            StructField("path", StringType, true),
            StructField("referer", StringType, true),
            StructField("guid", LongType, true),
            StructField("flashVersion", StringType, true),
            StructField("jre", StringType, true),
            StructField("sr", StringType, true),
            StructField("sc", StringType, true),
            StructField("geographic", IntegerType, true),
            StructField("field19", StringType, true),
            StructField("field20", StringType, true),
            StructField("field21", StringType, true),
            StructField("field22", StringType, true),
            StructField("category", StringType, true),
            StructField("field24", StringType, true)
        ))

    // Read the text file without a header
    val df = spark.read
        .option("delimiter", "\t")
        .option("header", "false")
        .schema(schema)
        .csv("sample-text")

    // Write DataFrame to Parquet
    df.write.parquet("output/pageviewlog")

    // Perform other jobs here...

    // Extract the day from the timestamp
    val dfWithDay = df.withColumn("day", date_format(col("timeCreate"), "yyyy-MM-dd"))

    // Example for the first job (most accessed URL per GUID)
    val windowSpec = Window.partitionBy("guid", "day").orderBy(desc("count"))

    val mostAccessedUrl = df
      .groupBy("guid", "url", "day")
      .count()
      .withColumn("rank", row_number().over(windowSpec))
      .where($"rank" === 1)
      .select("guid", "url", "count")

    mostAccessedUrl.show()

    spark.stop()
}
