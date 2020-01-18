package com.project.etl

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object dim_price {
  case class Price(price_id: Int, min_price: Double, max_price: Double, prince_range: String)

  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("dim_price")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val prices_Paris_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline",value = true).
      csv("externaldata/ParisListings.csv").
      cache().
      select(substring($"price",2,10).cast("double").as("price"))

    val prices_Berlin_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline",value = true).
      csv("externaldata/BerlinListings.csv").
      cache().
      select(substring($"price",2,10).cast("double").as("price"))

    val prices_Madrid_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline",value = true).
      csv("externaldata/MadridListings.csv").
      cache().
      select(substring($"price",2,10).cast("double").as("price"))


    prices_Madrid_DS.printSchema()
    prices_Madrid_DS.show()

    val all_prices_DS = prices_Paris_DS.
      union(prices_Berlin_DS).
      union(prices_Madrid_DS)

    val percentiles = all_prices_DS.stat.approxQuantile("price", Array(0, 0.25, 0.5, 0.75, 1), 0.1)

    val prices = Seq(
      Price(1, percentiles(0), percentiles(1)-1, "very cheap"),
      Price(2, percentiles(1), percentiles(2)-1, "cheap"),
      Price(3, percentiles(2), percentiles(3)-1, "average"),
      Price(4, percentiles(3), percentiles(4), "expensive")
    ).toDS()

    prices.show()

    prices.write.insertInto("etl_hd.dim_price")
  }

}
