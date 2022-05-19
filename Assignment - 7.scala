// Databricks notebook source
// MAGIC %md
// MAGIC ### Assignment – 7 (Spark & Spark SQL)

// COMMAND ----------

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// COMMAND ----------

// Disable Logs
Logger.getLogger("org").setLevel(Level.OFF)
val spark = SparkSession.builder().appName("WebLog").master("local[*]").getOrCreate()

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

val logs_DF = spark.read.option("header","true").csv("dbfs:/FileStore/shared_uploads/sayana.prakashan@ust.com/Weblog.csv")
logs_DF.printSchema()

// COMMAND ----------

logs_DF.show(10, false)

// COMMAND ----------

val s_ip = logs_DF.select(regexp_extract($"IP", """^(\d{2}.\d{3}.\d.\d)""", 1).alias("Source_IP"))
s_ip.show(5, false)

// COMMAND ----------

val timest = logs_DF.select(regexp_extract($"Time", """(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""", 1).alias("Timestamp"))
timest.show(5, false)

// COMMAND ----------

val Method = logs_DF.select(regexp_extract($"URL", """^([A-Z]{3,4})""", 1).alias("HTTP_Method"))
Method.show(5, false)

// COMMAND ----------

val req_url = logs_DF.select(regexp_extract($"URL", """^([A-Z]{3,4})\s(\S*)""", 2).alias("Request_URL"))
req_url.show(5, false)

// COMMAND ----------

val protocol = logs_DF.select(regexp_extract($"URL", """^([A-Z]{3,4})\s(\S*)\s(\S*)""", 3).alias("HTTP_Protocol"))
protocol.show(5, false)

// COMMAND ----------

// ^([A-Z]{3}[^\w])
val status = logs_DF.select(regexp_extract($"Staus", """^(\d{3})$""", 1).alias("Status_Code"))
status.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### a) Parsing the Log Files using RegExp & Pre-process Raw Log Data into Data frame with attributes.
// MAGIC             Source IP / Host
// MAGIC             Timestamp
// MAGIC             HTTP Method
// MAGIC             Request URL
// MAGIC             HTTP Protocol
// MAGIC             Status Code

// COMMAND ----------

val weblog_df = logs_DF.select(regexp_extract($"IP", """^(\d{2}.\d{3}.\d.\d)""", 1).alias("Source_IP"),
                           regexp_extract($"Time", """(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""", 1).alias("Timestamp"),
                           regexp_extract($"URL", """^([A-Z]{3,4})""", 1).alias("HTTP_Method"),
                           regexp_extract($"URL", """^([A-Z]{3,4})\s(\S*)""", 2).alias("Request_URL"),
                           regexp_extract($"URL", """^([A-Z]{3,4})\s(\S*)\s(\S*)""", 3).alias("HTTP_Protocol"),
                           regexp_extract($"Staus", """^(\d{3})$""", 1).cast("int").alias("Status_Code"))
weblog_df.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### b) Use data cleaning: count null and remove null values. Fix rows with null status (Drop those rows).

// COMMAND ----------

// Find Count of Null, None, NaN of all dataframe columns
import org.apache.spark.sql.functions.{col,when,count}
import org.apache.spark.sql.Column

def countNullCols (columns:Array[String]):Array[Column] = {
   columns.map(c => {
   count(when(col(c).isNull, c)).alias(c)
  })
}

weblog_df.select(countNullCols(weblog_df.columns): _*).show(false)

// COMMAND ----------

weblog_df.na.drop().show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### c) Pre-process and fix timestamp month name to month value. Convert Datetime (timestamp column) as Days, Month & Year.

// COMMAND ----------

// Parsing Timestamp
// Pre-process and fix timestamp month name to month value.

val month_map = Map("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8, "Sep" -> 9,
                   "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)
// UDF 
def parse_time(s : String):String = {
  "%3$s-%2$s-%1$s %4$s:%5$s:%6$s".format(s.substring(0,2), month_map(s.substring(3,6)), s.substring(7,11), 
                                             s.substring(12,14), s.substring(15,17), s.substring(18))
}

val toTimestamp = udf[String, String](parse_time(_))

val new_weblog_df = weblog_df.select($"*", to_timestamp(toTimestamp($"Timestamp")).alias("Datetime")).drop("Timestamp")
new_weblog_df.show(10, false)

// COMMAND ----------

// Convert Datetime (timestamp column) as Days, Month & Year.
val final_df = new_weblog_df.withColumn("Days",dayofmonth($"Datetime")).withColumn("Month",month($"Datetime")).withColumn("Year",year($"Datetime")).drop("Datetime")                   
final_df.show(10, false)

// COMMAND ----------

final_df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### d) Create new parquet file using cleaned Data Frame. Read the parquet file.

// COMMAND ----------

// Creating new parquet file using cleaned Data Frame.
// weblog_df.write.parquet("dbfs:/FileStore/shared_uploads/sayana.prakashan@ust.com/weblog_1/")

// COMMAND ----------

// Read the parquet file
val parque_log = spark.read.parquet("dbfs:/FileStore/shared_uploads/sayana.prakashan@ust.com/weblog_1/")
parque_log.show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### e) Show the summary of each column.

// COMMAND ----------

parque_log.summary().show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### f) Display frequency of 200 status code in the response for each month.

// COMMAND ----------

final_df.filter($"Status_Code" === "200").groupBy("Month").count().sort(desc("count")).show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### g) Frequency of Host Visits in November Month.

// COMMAND ----------

parque_log.createOrReplaceTempView("weblog_table")

// COMMAND ----------

spark.sql("select Source_IP, count(*) as Count from weblog_table WHERE Timestamp like '%Nov%' group by Source_IP order by Count desc").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### h) Display Top 15 Error Paths - status != 200.

// COMMAND ----------

spark.sql("select Request_URL, count(*) as Count from weblog_table WHERE Status_Code != 200 group by Request_URL order by Count desc").show(15, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### i) Display Top 10 Paths with Error - with status equals 200.

// COMMAND ----------

spark.sql("select Request_URL, count(*) as Count from weblog_table WHERE Status_Code = 200 group by Request_URL order by Count desc").show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### j) Exploring 404 status code. Listing 404 status Code Records. List Top 20 Host with 404 response status code (Query + Visualization).

// COMMAND ----------

spark.sql("select Source_IP, count(*) as Count from weblog_table WHERE Status_Code = 404 group by Source_IP order by Count desc limit 20").show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC select Source_IP, count(*) as Count from weblog_table WHERE Status_Code = 404 group by Source_IP order by Count desc limit 20

// COMMAND ----------

// MAGIC %md
// MAGIC ##### k) Display the List of 404 Error Response Status Code per Day (Query + Visualization).

// COMMAND ----------

final_df.createOrReplaceTempView("weblog_table2")

// COMMAND ----------

spark.sql("select Days, count(*) as Count from weblog_table2 WHERE Status_Code = 404 group by Days order by Days").show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC select Days, count(*) as Count from weblog_table2 WHERE Status_Code = 404 group by Days order by Days

// COMMAND ----------

// MAGIC %md
// MAGIC ##### l) List Top 20 Paths (Endpoint) with 404 Response Status Code.

// COMMAND ----------

spark.sql("select Request_URL, count(*) as Count from weblog_table WHERE Status_Code = 404 group by Request_URL order by Count desc").show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC select Request_URL, count(*) as Count from weblog_table WHERE Status_Code = 404 group by Request_URL order by Count desc

// COMMAND ----------

// MAGIC %md
// MAGIC ##### m) Query to Display Distinct Path responding 404 in status error.

// COMMAND ----------

spark.sql("select distinct(Request_URL) from weblog_table WHERE Status_Code = 404").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### n) Find the number of unique source IPs that have made requests to the webserver for each month.

// COMMAND ----------

spark.sql("select Source_IP, count(Source_IP) as Count from (select Month, Source_IP from weblog_table2 group by Month, Source_IP) group by Source_IP").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### o) Display the top 20 requested Paths in each Month (Query + Visualization).

// COMMAND ----------

spark.sql("select Month, count(Source_IP) as Count from weblog_table2 group by Month order by Count desc limit 20").show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### p) Query to Display Distinct Path responding 404 in status error.

// COMMAND ----------

spark.sql("select distinct(Request_URL) from weblog_table WHERE Status_Code = 404").show(false)

// COMMAND ----------


