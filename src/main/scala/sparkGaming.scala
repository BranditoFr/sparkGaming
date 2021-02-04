import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import sparkGamingUtils._

object sparkGaming extends App{
  val lConf = ConfigFactory.load("sparkGaming.conf")

  val lOutputTwitchEsportByMonth = lConf.getString("output.twitch_esport_by_month")

  val lDatasetEsportHistorical = lConf.getString("dataset.esport_historical")
  val lDatasetEsportGeneral = lConf.getString("dataset.esport_general")
  val lDatasetTwitchGameData = lConf.getString("dataset.twitch_game_data")
  val lDatasetTwitchGlobalData = lConf.getString("dataset.twitch_global_data")

  val spark =
    SparkSession
      .builder()
      .appName("Spark Gaming")
      .master("local[*]")
      .getOrCreate()

  val lDfEsportHistorical: DataFrame = readFromCsv(lDatasetEsportHistorical)
  lDfEsportHistorical.show(false)

  val lDfEsportGeneral: DataFrame = readFromCsv(lDatasetEsportGeneral)
  lDfEsportGeneral.show(false)

  val lDfTwitchGameData: DataFrame = readFromCsv(lDatasetTwitchGameData)
  lDfTwitchGameData.show(false)

  val lDfTwitchGlobalData: DataFrame = readFromCsv(lDatasetTwitchGlobalData)
  lDfTwitchGlobalData.show(false)

  val lDfTwitchEsportByMonth: DataFrame =
    lDfEsportHistorical
      .filter(col("date") >= "2015-01-01")
      .withColumn("year2", functions.split(col("date"), "-").getItem(0))
      .withColumn("month2", functions.split(col("date"), "-").getItem(1))
      .withColumn("day", functions.split(col("date"), "-").getItem(2))
      .withColumnRenamed("game", "game2")
      .join(lDfTwitchGameData,
        col("game2") === col("game") &&
          col("year2") === col("year") &&
          col("month2") === col("month"),
        "inner"
      )
      .drop("game2", "year2", "month2")

  lDfTwitchEsportByMonth.show(false)


  // Moyenne sur les 5 dernieres années sur le jeu
  // Trier tout les jeux par genre (esport)
  saveCsvFromDataframe(lOutputTwitchEsportByMonth, lDfTwitchEsportByMonth)
}
