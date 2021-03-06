package com.project.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object fact {

  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("f_fact")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    ////////////////////////////////////////////// Paris

    val listings_Paris_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline", value = true).
      csv("externaldata/ParisListings.csv").
      cache().
      select(
        $"id", $"bathrooms", $"bedrooms", $"review_scores_location", $"review_score_value",
        substring($"price", 2, 10).cast("double").as("price"),
        concat($"country_code", $"zipcode").as("location_id"))

    listings_Paris_DS.show()

    val calendar_Paris_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      csv("externaldata/ParisCalendar.csv").
      select($"listing_id", $"date", $"available")
    calendar_Paris_DS.show()


    val dim_location_score = spark.sql("select * from dim_location_score")
    val dim_prices = spark.sql("select * from dim_price")


    val facts_Paris = calendar_Paris_DS.join(listings_Paris_DS, $"listing_id" === $"id").
      join(dim_location_score, $"review_scores_location >= min_location_score && location_score <= min_location_score").
      join(dim_prices, $"price >= min_price && price <= max_price").
      select(
        $"date".as("dt"),
        $"location_id",
        $"price_id",
        $"location_score_id",
        $"price",
        $"review_score_value",
        $"bathrooms", $"bedrooms",
        $"available"
      ).
      groupBy($"dt", $"location_id", $"price_id",
        $"location_score_id",
        $"bathrooms", $"bedrooms").
      agg(sum("price").as("sum_price"),
        sum("review_score_value").as("sum_review_score"),
        count(when($"available" === 't', true)).as("count_available"),
        count(when($"available" === 'f', true)).as("count_not_available")).
      drop("available", "price", "review_score_value")

    ////////////////////////////////////////////// Berlin

    val listings_Berlin_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline", value = true).
      csv("externaldata/BerlinListings.csv").
      cache().
      select(
        $"id", $"bathrooms", $"bedrooms", $"review_scores_location", $"review_score_value",
        substring($"price", 2, 10).cast("double").as("price"),
        concat($"country_code", $"zipcode").as("location_id"))

    listings_Berlin_DS.show()

    val calendar_Berlin_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      csv("externaldata/BerlinCalendar.csv").
      select($"listing_id", $"date", $"available")
    calendar_Berlin_DS.show()

    val facts_Berlin = calendar_Berlin_DS.join(listings_Berlin_DS, $"listing_id" === $"id").
      join(dim_location_score, $"review_scores_location >= min_location_score && location_score <= min_location_score").
      join(dim_prices, $"price >= min_price && price <= max_price").
      select(
        $"date".as("dt"),
        $"location_id",
        $"price_id",
        $"location_score_id",
        $"price",
        $"review_score_value",
        $"bathrooms", $"bedrooms",
        $"available"
      ).
      groupBy($"dt", $"location_id", $"price_id",
        $"location_score_id",
        $"bathrooms", $"bedrooms").
      agg(sum("price").as("sum_price"),
        sum("review_score_value").as("sum_review_score"),
        count(when($"available" === 't', true)).as("count_available"),
        count(when($"available" === 'f', true)).as("count_not_available")).
      drop("available", "price", "review_score_value")

    ////////////////////////////////////////////// Madrid

    val listings_Madrid_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      option("quote", "\"").
      option("escape", "\"").
      option("delimiter", ",").
      option("multiline", value = true).
      csv("externaldata/MadridListings.csv").
      cache().
      select(
        $"id", $"bathrooms", $"bedrooms", $"review_scores_location", $"review_score_value",
        substring($"price", 2, 10).cast("double").as("price"),
        concat($"country_code", $"zipcode").as("location_id"))

    listings_Madrid_DS.show()

    val calendar_Madrid_DS = spark.read.format("org.apache.spark.csv").
      option("header", value = true).option("inferSchema", value = true).
      csv("externaldata/MadridCalendar.csv").
      select($"listing_id", $"date", $"available")
    calendar_Madrid_DS.show()

    val facts_Madrid = calendar_Madrid_DS.join(listings_Madrid_DS, $"listing_id" === $"id").
      join(dim_location_score, $"review_scores_location >= min_location_score && location_score <= min_location_score").
      join(dim_prices, $"price >= min_price && price <= max_price").
      select(
        $"date".as("dt"),
        $"location_id",
        $"price_id",
        $"location_score_id",
        $"price",
        $"review_score_value",
        $"bathrooms", $"bedrooms",
        $"available"
      ).
      groupBy($"dt", $"location_id", $"price_id",
        $"location_score_id",
        $"bathrooms", $"bedrooms").
      agg(sum("price").as("sum_price"),
        sum("review_score_value").as("sum_review_score"),
        count(when($"available" === 't', true)).as("count_available"),
        count(when($"available" === 'f', true)).as("count_not_available")).
      drop("available", "price", "review_score_value")

    val facts_all = facts_Madrid.union(facts_Paris).union(facts_Berlin)

    facts_all.write.insertInto("etl_hd.fact")
  }

}
