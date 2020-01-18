package com.project.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dim_location {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("dim_location")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val location_Berlin_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline",value = true).
      csv("externaldata/BerlinListings.csv").
      select($"country_code", $"country", $"city", $"zipcode", concat($"country_code", $"zipcode").as("location_id")).
      distinct().
      cache()

    val location_Madrid_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline",value = true).
      csv("externaldata/MadridListings.csv").
      select($"country_code", $"country", $"city", $"zipcode", concat($"country_code", $"zipcode").as("location_id")).
      distinct().
      cache()

    val location_Paris_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline",value = true).
      csv("externaldata/ParisListings.csv").
      select($"country_code", $"country", $"city", $"zipcode", concat($"country_code", $"zipcode").as("location_id")).
      distinct().
      cache()

    val all_location = location_Berlin_DS.
      union(location_Madrid_DS).
      union(location_Paris_DS)

    all_location.write.insertInto("etl_hd.dim_location")
  }
}
