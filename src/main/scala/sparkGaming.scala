import org.apache.spark

import java.io.File
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import sparkGamingUtils._

object sparkGaming extends App{

  val lDatasetEsportHistorical = "D:\\M1\\Spark\\Projet\\sparkGaming\\src\\main\\ressources\\data\\esport\\HistoricalEsportData.csv"
  val lDatasetEsportGeneral = "D:\\M1\\Spark\\Projet\\sparkGaming\\src\\main\\ressources\\data\\esport\\GeneralEsportData.csv"
  val lDatasetTwitchGameData = "D:\\M1\\Spark\\Projet\\sparkGaming\\src\\main\\ressources\\data\\twitch\\Twitch_game_data.csv"
  val lDatasetTwitchGlobalData = "D:\\M1\\Spark\\Projet\\sparkGaming\\src\\main\\ressources\\data\\twitch\\Twitch_global_data.csv"

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
      .drop("game2","year2","month2")

  lDfTwitchEsportByMonth.show(false)

}
