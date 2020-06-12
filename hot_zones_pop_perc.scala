// Databricks notebook source
import org.apache.spark.sql.functions._

val cleanCounty = udf((c: String) => {
  val i = c.indexOf(" County")
  if (i == -1){
    c
  } else {
   c.substring(0, i) 
  }
})

// COMMAND ----------

val d = "dbfs:/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_time_series/"
val csse_ts_us_c = spark.read.option("header", "true").option("inferSchema", "true")
              .csv(d + "time_series_covid19_confirmed_US.csv")
              .withColumnRenamed("Province_State", "State")
              .withColumnRenamed("Admin2", "County")
val csse_ts_us_d = spark.read.option("header", "true")
              .csv(d + "time_series_covid19_deaths_US.csv")

val csse_ts_g_c = spark.read.option("header", "true")
              .csv(d + "time_series_covid19_confirmed_global.csv")
val csse_ts_g_d = spark.read.option("header", "true")
              .csv(d + "time_series_covid19_deaths_global.csv")
val csse_ts_g_r = spark.read.option("header", "true")
              .csv(d + "time_series_covid19_recovered_global.csv")

// COMMAND ----------

display(csse_ts_us_c)

// COMMAND ----------

display(csse_ts_g_c)

// COMMAND ----------

// MAGIC %sh
// MAGIC mkdir /dbfs/tmp/tigran
// MAGIC wget -O /dbfs/tmp/tigran/co-est2019-alldata.csv https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv

// COMMAND ----------

val census_county = spark.read.option("header", "true").option("inferSchema", "true")
              .csv("/tmp/tigran/co-est2019-alldata.csv")
              .withColumnRenamed("STATE", "State_cd")
              .withColumnRenamed("COUNTY", "County_cd")
              .withColumnRenamed("STNAME", "State")
              .withColumnRenamed("CTYNAME", "County")

// COMMAND ----------

display(census_county)

// COMMAND ----------

val census_county_cleaned = census_county.filter('County_cd =!= "000")
                                         .withColumn("County", cleanCounty('County))
                                         .select("State", "County", "POPESTIMATE2019") // 3142
val csse_ts_us_c_cleaned = csse_ts_us_c.filter(!'County.startsWith("Out of ") && 'County =!= "Unassigned") // 3152
// make sure we have a good match on State, County
val c_c_g = census_county_cleaned
              .groupBy('State).agg(count('County).as("c_rec_count"))
val ts_c_g = csse_ts_us_c_cleaned
              .groupBy('State).agg(count('County).as("ts_rec_count"))
val cc = c_c_g.join(broadcast(ts_c_g), Seq("State"), "left").withColumn("diff", 'c_rec_count - 'ts_rec_count) // 51 - ts has a 10 more "County" values in 4 states
display(cc)

// COMMAND ----------

val cases_ts_us_with_pop = census_county_cleaned.join(broadcast(csse_ts_us_c_cleaned), Seq("State", "County"), "left").persist
display(cases_ts_us_with_pop)

// COMMAND ----------

val allCols = csse_ts_us_c_cleaned.columns
val lastIndex = allCols.size - 1
val (lastDate, minus7Date, minus14Date, minus30Date) = (allCols(lastIndex), allCols(lastIndex - 7), allCols(lastIndex - 14), allCols(lastIndex - 30))

// COMMAND ----------

val cases_ts_us_with_pop_stats_prep = cases_ts_us_with_pop.withColumn("last_30", col(lastDate) - col(minus30Date))
                                                     .withColumn("last_14", col(lastDate) - col(minus14Date))
                                                     .withColumn("last_7", col(lastDate) - col(minus7Date))
                                                     .withColumn("per_pop_confirmed", col(lastDate) * 100 / 'POPESTIMATE2019)
                                                     .withColumn("per_pop_confirmed_30", 'last_30 * 100 / 'POPESTIMATE2019)
                                                     .withColumn("per_pop_confirmed_14", 'last_14 * 100 / 'POPESTIMATE2019)
                                                     .withColumn("per_pop_confirmed_7", 'last_7 * 100 / 'POPESTIMATE2019)
                                                     .withColumn("rate_increase_30", 'per_pop_confirmed_30 / ('per_pop_confirmed - 'per_pop_confirmed_30))
                                                     .withColumn("rate_increase_14", 'per_pop_confirmed_14 / ('per_pop_confirmed - 'per_pop_confirmed_14))
                                                     .withColumn("rate_increase_7", 'per_pop_confirmed_7 / ('per_pop_confirmed - 'per_pop_confirmed_7))
                                                     .withColumn("tangent_30", 'per_pop_confirmed_30 / lit(30))
                                                     .withColumn("tangent_14", 'per_pop_confirmed_14 / lit(14))
                                                     .withColumn("tangent_7", 'per_pop_confirmed_7 / lit(7))
                                                     .withColumn("trend", when('tangent_30 <= 'tangent_14 && 'tangent_14 <= 'tangent_7, "rising")
                                                                         .when('tangent_30 > 'tangent_14 && 'tangent_14 > 'tangent_7, "lowering")
                                                                         .otherwise("leveling"))
val (colsLen, newColsStart) = (cases_ts_us_with_pop_stats_prep.columns.size, cases_ts_us_with_pop_stats_prep.columns.indexOf(lastDate))
val cols = Seq(minus30Date, minus14Date, minus7Date) ++ 
            cases_ts_us_with_pop_stats_prep.columns.slice(newColsStart, colsLen) ++ 
            (cases_ts_us_with_pop_stats_prep.columns.slice(0, newColsStart).toBuffer -- Seq(minus30Date, minus14Date, minus7Date))
val cases_ts_us_with_pop_stats = cases_ts_us_with_pop_stats_prep.select(cols.map(col(_)): _*)
display(cases_ts_us_with_pop_stats)

// COMMAND ----------

val hot_zones = cases_ts_us_with_pop_stats.filter('trend === "rising" && ('per_pop_confirmed_30 >= 1 || 'per_pop_confirmed_14 >= 1 || 'per_pop_confirmed_7 >= 1)) // 14
display(hot_zones)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------


