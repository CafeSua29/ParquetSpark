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
            StructField("url", StringType, true),
            StructField("field22", StringType, true),
            StructField("category", StringType, true),
            StructField("field24", StringType, true)
        ))

    // Read the text file
    val df = spark.read
        .option("delimiter", "\t")
        .option("header", "false")
        .schema(schema)
        .csv("sample-text")

    // Write DataFrame to Parquet
    df.write.mode("overwrite").parquet("output/pageviewlog")

    // Perform jobs 

    // Extract the day from the timestamp
    val dfWithDay = df.withColumn("day", date_format(col("timeCreate"), "yyyy-MM-dd"))

    // 3.1
    val windowSpec = Window.partitionBy("guid", "day").orderBy(desc("count"))

    val mostAccessedUrl = dfWithDay
      .groupBy("guid", "url", "day")
      .count()
      .withColumn("rank", row_number().over(windowSpec))
      .where($"rank" === 1)
      .select("guid", "url", "count")

    mostAccessedUrl.show()

    // 3.2
    val ipUsage = df.groupBy("ip")
    .agg(countDistinct("guid").as("uniqueGuids"))
    .orderBy(desc("uniqueGuids"))
    .limit(1000)

    ipUsage.show()

    // 3.3
    val topDomains = df.groupBy("domain")
    .count()
    .orderBy(desc("count"))
    .limit(100)

    topDomains.show()

    // 3.4
    val topLocIds = df.groupBy("locId")
    .agg(countDistinct("ip").as("uniqueIps"))
    .orderBy(desc("uniqueIps"))
    .limit(10)

    topLocIds.show()

    // 3.5
    val popularBrowsers = df.groupBy("osCode", "browserCode")
    .count()
    .withColumn("rank", row_number().over(Window.partitionBy("osCode").orderBy(desc("count"))))
    .filter(col("rank") === 1)
    .select("osCode", "browserCode")

    popularBrowsers.show()

    // 3.6
    val filteredData = df.filter(unix_timestamp(col("timeCreate")) - unix_timestamp(col("cookieCreate")) > 600)
    .select("guid", "domain", "path", "timeCreate")

    // Save the result to a text file
    filteredData
    .coalesce(1) 
    .write
    .mode("overwrite") 
    .option("header", "false") 
    .format("text") 
    .save("output/result.dat")

    spark.stop()
}
