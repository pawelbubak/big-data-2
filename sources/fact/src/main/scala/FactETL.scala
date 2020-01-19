import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FactETL {

    def main(args: Array[String]) {
        val spark: SparkSession = SparkSession.builder()
            .appName("Fact ETL")
            .enableHiveSupport()
            .getOrCreate()

        import spark.implicits._

        ////////////////////////////////////////////// Paris

        val listings_paris_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/ParisListings.csv").
            cache().
            select(
                $"id", $"bathrooms", $"bedrooms", $"review_scores_location", $"review_scores_value",
                substring($"price", 2, 10).cast("double").as("price"),
                concat($"zipcode", $"city").as("location_id"))

        val calendar_paris_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            csv("project/spark/ParisCalendar.csv").
            select($"listing_id", $"date", $"available")

        val dim_location_score = spark.sql("select * from etl_hd.dim_location_score")
        val dim_prices = spark.sql("select * from etl_hd.dim_price")


        val facts_paris = calendar_paris_DS.join(listings_paris_DS, $"listing_id" === $"id").
            join(dim_location_score, ($"review_scores_location" >= $"dim_location_score.min_location_score") &&
                ($"review_scores_location" <= $"dim_location_score.max_location_score")).
            join(dim_prices, $"price" >= $"min_price" && $"price" <= $"max_price").
            select(
                $"date".as("dt"),
                $"location_id",
                $"price_id",
                $"location_score_id",
                $"price",
                $"review_scores_value",
                $"bathrooms", $"bedrooms",
                $"available"
            ).
            groupBy($"dt", $"location_id", $"price_id",
                $"location_score_id",
                $"bathrooms", $"bedrooms").
            agg(sum("price").as("sum_price"),
                sum("review_scores_value").as("sum_review_score"),
                count(when($"available" === "t", 1)).as("count_available"),
                count(when($"available" === "f", 1)).as("count_not_available")).
            drop("available", "price", "review_scores_value")

        ////////////////////////////////////////////// Berlin

        val listings_berlin_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/BerlinListings.csv").
            cache().
            select(
                $"id", $"bathrooms", $"bedrooms", $"review_scores_location", $"review_scores_value",
                substring($"price", 2, 10).cast("double").as("price"),
                concat($"zipcode", $"city").as("location_id"))

        val calendar_berlin_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            csv("project/spark/BerlinCalendar.csv").
            select($"listing_id", $"date", $"available")

        val facts_berlin = calendar_berlin_DS.join(listings_berlin_DS, $"listing_id" === $"id").
            join(dim_location_score, ($"review_scores_location" >= $"dim_location_score.min_location_score") &&
                ($"review_scores_location" <= $"dim_location_score.max_location_score")).
            join(dim_prices, $"price" >= $"min_price" && $"price" <= $"max_price").
            select(
                $"date".as("dt"),
                $"location_id",
                $"price_id",
                $"location_score_id",
                $"price",
                $"review_scores_value",
                $"bathrooms", $"bedrooms",
                $"available"
            ).
            groupBy($"dt", $"location_id", $"price_id",
                $"location_score_id",
                $"bathrooms", $"bedrooms").
            agg(sum("price").as("sum_price"),
                sum("review_scores_value").as("sum_review_score"),
                count(when($"available" === "t", 1)).as("count_available"),
                count(when($"available" === "f", 1)).as("count_not_available")).
            drop("available", "price", "review_scores_value")

        ////////////////////////////////////////////// Madrid

        val listings_madrid_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/MadridListings.csv").
            cache().
            select(
                $"id", $"bathrooms", $"bedrooms", $"review_scores_location", $"review_scores_value",
                substring($"price", 2, 10).cast("double").as("price"),
                concat($"zipcode", $"city").as("location_id"))

        val calendar_madrid_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            csv("project/spark/MadridCalendar.csv").
            select($"listing_id", $"date", $"available")

        val facts_madrid = calendar_madrid_DS.join(listings_madrid_DS, $"listing_id" === $"id").
            join(dim_location_score, ($"review_scores_location" >= $"dim_location_score.min_location_score") &&
                ($"review_scores_location" <= $"dim_location_score.max_location_score")).
            join(dim_prices, $"price" >= $"min_price" && $"price" <= $"max_price").
            select(
                $"date".as("dt"),
                $"location_id",
                $"price_id",
                $"location_score_id",
                $"price",
                $"review_scores_value",
                $"bathrooms", $"bedrooms",
                $"available"
            ).
            groupBy($"dt", $"location_id", $"price_id",
                $"location_score_id",
                $"bathrooms", $"bedrooms").
            agg(sum("price").as("sum_price"),
                sum("review_scores_value").as("sum_review_score"),
                count(when($"available" === "t", 1)).as("count_available"),
                count(when($"available" === "f", 1)).as("count_not_available")).
            drop("available", "price", "review_scores_value")

        val facts_all = facts_madrid.union(facts_paris).union(facts_berlin)

        facts_all.write.insertInto("etl_hd.f_fact")
    }

}
