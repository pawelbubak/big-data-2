import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LocationETL {

    def main(args: Array[String]) {
        val spark: SparkSession = SparkSession.builder()
            .appName("Location ETL")
            .getOrCreate()

        import spark.implicits._

        val location_berlin_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("externaldata/BerlinListings.csv").
            select($"country_code", $"country", $"city", $"zipcode", concat($"country_code", $"zipcode").as("location_id")).
            distinct().
            cache()

        val location_madrid_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("externaldata/MadridListings.csv").
            select($"country_code", $"country", $"city", $"zipcode", concat($"country_code", $"zipcode").as("location_id")).
            distinct().
            cache()

        val location_paris_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("externaldata/ParisListings.csv").
            select($"country_code", $"country", $"city", $"zipcode", concat($"country_code", $"zipcode").as("location_id")).
            distinct().
            cache()

        val all_location = location_berlin_DS.
            union(location_madrid_DS).
            union(location_paris_DS)

        all_location.write.insertInto("etl_hd.dim_location")
    }

}
