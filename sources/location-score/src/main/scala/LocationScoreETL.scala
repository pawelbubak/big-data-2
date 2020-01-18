import org.apache.spark.sql.SparkSession

object LocationScoreETL {

    case class LocationScore(location_score_id: Int, min_location_score: Int, max_location_score: Int, location_score_range: String)

    def main(args: Array[String]) {
        val spark: SparkSession = SparkSession.builder()
            .appName("SparkByExample")
            .getOrCreate()

        import spark.implicits._

        val location_score_DS = Seq(
            LocationScore(1, 1, 2, "terrible"),
            LocationScore(2, 3, 4, "bad"),
            LocationScore(3, 5, 6, "average"),
            LocationScore(4, 7, 8, "good"),
            LocationScore(5, 9, 10, "excellent")
        ).toDS()

        location_score_DS.write.insertInto("etl_hd.dim_location_score")
    }

}
