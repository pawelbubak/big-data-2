import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TimeETL {

    case class Time(date: Timestamp, day: Int, month: Int, year: Int)

    def main(args: Array[String]) {
        val spark: SparkSession = SparkSession.builder()
            .appName("Time ETL")
            .enableHiveSupport()
            .getOrCreate()

        import spark.implicits._

        val paris_calendar_DS = spark.read.
            format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            csv("project/spark/ParisCalendar.csv").
            cache().agg(min($"date").as("min_date"), max($"date").as("max_date"))

        val madrid_calendar_DS = spark.read.
            format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            csv("project/spark/MadridCalendar.csv").
            cache().agg(min($"date").as("min_date"), max($"date").as("max_date"))

        val berlin_calendar_DS = spark.read.
            format("org.apache.spark.csv").
            option("header", value = true).
            option("inferSchema", value = true).
            csv("project/spark/BerlinCalendar.csv").
            cache().agg(min($"date").as("min_date"), max($"date").as("max_date"))

        val calendar_DS = paris_calendar_DS.union(madrid_calendar_DS).union(berlin_calendar_DS)

        val dates = calendar_DS.agg(min($"min_date").as("min_date"), max($"max_date").as("max_date")).first()

        val min_date = dates.getTimestamp(0)
        val max_date = dates.getTimestamp(1)
        var actual_date = min_date

        var dates_DS = spark.emptyDataset[Time]

        while (actual_date.compareTo(max_date) <= 0) {
            val c = Calendar.getInstance()
            c.setTime(actual_date)
            dates_DS = dates_DS.union(Seq(Time(
                actual_date,
                c.get(Calendar.DAY_OF_MONTH),
                c.get(Calendar.MONTH) + 1,
                c.get(Calendar.YEAR)
            )).toDS())
            c.add(Calendar.DATE, 1)
            actual_date = new Timestamp(c.getTimeInMillis)
        }

        dates_DS.write.insertInto("etl_hd.dim_time")
    }

}
