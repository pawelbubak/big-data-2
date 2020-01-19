import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LocationETL {

    def main(args: Array[String]) {
        val spark: SparkSession = SparkSession.builder()
            .appName("Location ETL")
            .enableHiveSupport()
            .getOrCreate()

        import spark.implicits._

        val location_berlin_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/BerlinListings.csv").
            select(upper($"country_code"), upper($"country"), upper($"city"),
                $"zipcode", concat($"zipcode", $"city").as("location_id")).
            where($"country_code" === "DE").
            distinct().
            cache()

        val location_madrid_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/MadridListings.csv").
            select(upper($"country_code"), upper($"country"), upper($"city"),
                $"zipcode", concat($"zipcode", $"city").as("location_id")).
            where($"country_code" === "ES").
            distinct().
            cache()

        val location_paris_DS = spark.read.format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            option("quote", "\"").
            option("escape", "\"").
            option("delimiter", ",").
            option("multiline", value = true).
            csv("project/spark/ParisListings.csv").
            select(upper($"country_code"), upper($"country"), upper($"city"),
                $"zipcode", concat($"zipcode", $"city").as("location_id")).
            where($"country_code" === "FR").
            distinct().
            cache()

        val all_location = location_berlin_DS.
            union(location_madrid_DS).
            union(location_paris_DS)

        all_location.write.insertInto("etl_hd.dim_location")
    }

}
