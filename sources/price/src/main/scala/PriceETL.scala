import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PriceETL {

    case class Price(price_id: Int, min_price: Double, max_price: Double, prince_range: String)

    def main(args: Array[String]) {
        val spark: SparkSession = SparkSession.builder()
            .appName("Price ETL")
            .enableHiveSupport()
            .getOrCreate()

        import spark.implicits._

        val prices_paris_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/ParisListings.csv").
            cache().
            select(substring($"price", 2, 10).cast("double").as("price"))

        val prices_berlin_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/BerlinListings.csv").
            cache().
            select(substring($"price", 2, 10).cast("double").as("price"))

        val prices_madrid_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/MadridListings.csv").
            cache().
            select(substring($"price", 2, 10).cast("double").as("price"))

        val all_prices_DS = prices_paris_DS.
            union(prices_berlin_DS).
            union(prices_madrid_DS)

        val percentiles = all_prices_DS.stat.approxQuantile("price", Array(0, 0.25, 0.5, 0.75, 1), 0.1)

        val prices = Seq(
            Price(1, percentiles(0), percentiles(1) - 1, "very cheap"),
            Price(2, percentiles(1), percentiles(2) - 1, "cheap"),
            Price(3, percentiles(2), percentiles(3) - 1, "average"),
            Price(4, percentiles(3), percentiles(4), "expensive")
        ).toDS()

        prices.write.insertInto("etl_hd.dim_price")
    }

}
