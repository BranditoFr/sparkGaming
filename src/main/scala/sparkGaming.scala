
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, bround, col, sum}
import sparkGamingUtils._
import StaticStrings._
import org.apache.spark.sql.types.IntegerType

object sparkGaming {

  val mSpark: SparkSession =
    SparkSession
      .builder()
      .appName("Spark Gaming")
      .master("local[*]")
      .getOrCreate()

  mSpark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    /** set input parameters */
    val lDateMin: String = args(0)

    /** get input path from conf file */
    val lConf: Config = ConfigFactory.load("sparkGaming.conf")

    val lOutputPathTwitchEsportByMonth: String = lConf.getString("output.twitch_esport_by_month")
    val lOutputPathStatsEsportByGenre: String = lConf.getString("output.stats_esport_by_genre")

    val lDatasetEsportHistorical: String = lConf.getString("dataset.esport_historical")
    val lDatasetEsportGeneral: String = lConf.getString("dataset.esport_general")
    val lDatasetTwitchGameData: String = lConf.getString("dataset.twitch_game_data")
    val lDatasetTwitchGlobalData: String = lConf.getString("dataset.twitch_global_data")

    /** set DataFrame from source */
    val lEsportHistoricalDF: DataFrame = readFromCsv(lDatasetEsportHistorical)
    lEsportHistoricalDF.show(false)

    val lEsportGeneralDF: DataFrame = readFromCsv(lDatasetEsportGeneral)
    lEsportGeneralDF.show(false)

    val lTwitchGameDataDF: DataFrame = readFromCsv(lDatasetTwitchGameData)
    lTwitchGameDataDF.show(false)

    val lTwitchGlobalDataDF: DataFrame = readFromCsv(lDatasetTwitchGlobalData)
    lTwitchGlobalDataDF.show(false)

    /** filter earning by esport genre */
    val lEsportByGenreDF: DataFrame =
      lEsportGeneralDF
        .withColumn(sTotalEarnings, col(sTotalEarnings).cast(IntegerType))
        .withColumn(sOnlineEarnings, col(sOnlineEarnings).cast(IntegerType))
        .withColumn(sTotalPlayers, col(sTotalPlayers).cast(IntegerType))
        .withColumn(sTotalTournaments, col(sTotalTournaments).cast(IntegerType))
        .groupBy(sGenre)
        .agg(
          sum(sTotalEarnings).alias(s"""sum_$sTotalEarnings"""),
          sum(sOnlineEarnings).alias(s"""sum_$sOnlineEarnings"""),
          sum(sTotalPlayers).alias(s"""sum_$sTotalPlayers"""),
          sum(sTotalTournaments).alias(s"""sum_$sTotalTournaments""")
        )

    lEsportByGenreDF.show(false)

    saveCsvFromDataframe(lOutputPathStatsEsportByGenre, lEsportByGenreDF)

    /**  stats twitch and esport join by month */
    val lTwitchEsportByMonthDF: DataFrame =
      lEsportHistoricalDF
        .filter(col(sDate) >= lDateMin)
        .withColumn("year2", functions.split(col(sDate), "-").getItem(0))
        .withColumn("month2", functions.split(col(sDate), "-").getItem(1))
        .withColumn("day", functions.split(col(sDate), "-").getItem(2))
        .withColumnRenamed(sGame, "game2")
        .join(lTwitchGameDataDF,
          col("game2") === col(sGame) &&
            col("year2") === col(sYear) &&
            col("month2") === col(sMonth),
          "inner"
        )
        .drop("game2", "year2", "month2")

    lTwitchEsportByMonthDF.show(false)

    saveCsvFromDataframe(lOutputPathTwitchEsportByMonth, lTwitchEsportByMonthDF)

    /** average on games by year (esport + twitch) */
    // Par game + année OU/ET que par année OU/ET que par jeu ????

    val lAverageByYearDF: DataFrame =
      lTwitchEsportByMonthDF
        .withColumn(sEarnings, col(sEarnings).cast(IntegerType))
        .withColumn(sPlayers, col(sPlayers).cast(IntegerType))
        .withColumn(sTournaments, col(sTournaments).cast(IntegerType))
        .withColumn(sHoursWatched, col(sHoursWatched).cast(IntegerType))
        .withColumn(sHoursStreamed, col(sHoursStreamed).cast(IntegerType))
        .groupBy(sYear, sGame)
        .agg(
          bround(sum(sEarnings),0).alias(s"""sum_$sEarnings"""),
          bround(sum(sPlayers),0).alias(s"""sum_$sPlayers"""),
          bround(sum(sTournaments),0).alias(s"""sum_$sTournaments"""),
          bround(sum(sHoursWatched),0).alias(s"""sum_$sHoursWatched"""),
          bround(sum(sHoursStreamed),0).alias(s"""sum_$sHoursStreamed"""),
          bround(avg(sEarnings),0).alias(s"""avg_$sEarnings"""),
          bround(avg(sPlayers),0).alias(s"""avg_$sPlayers"""),
          bround(avg(sTournaments),0).alias(s"""avg_$sTournaments"""),
          bround(avg(sHoursWatched),0).alias(s"""avg_$sHoursWatched"""),
          bround(avg(sHoursStreamed),0).alias(s"""avg_$sHoursStreamed""")
        )

    lAverageByYearDF.show(false)

    // essayer de rajouter les type de jeu d'esport dans le df de twitch pour trier par type sur twitch
    val lTwitchByGenreDF =
      lTwitchGameDataDF
        .withColumn(sHoursWatched, col(sHoursWatched).cast(IntegerType))
        .withColumn(sHoursStreamed, col(sHoursStreamed).cast(IntegerType))
        .join(lEsportGeneralDF.select(col(sGame) as "game2", col(sGenre)), col("game2") === col(sGame), "inner")
        .drop("game2")
        .groupBy(sGenre)
        .agg(
          bround(sum(sHoursWatched),0).alias(s"""sum_$sHoursWatched"""),
          bround(sum(sHoursStreamed),0).alias(s"""sum_$sHoursStreamed"""),
          bround(avg(sHoursWatched),0).alias(s"""avg_$sHoursWatched"""),
          bround(avg(sHoursStreamed),0).alias(s"""avg_$sHoursStreamed""")
        )

    lTwitchByGenreDF.show(false)

    val lDataByGenre: DataFrame =
      lTwitchByGenreDF
        .join(
          lEsportByGenreDF
            .select(
              col(sGenre) as "genre2",
              col(s"""sum_$sTotalEarnings"""),
              col(s"""sum_$sOnlineEarnings"""),
              col(s"""sum_$sTotalPlayers"""),
              col(s"""sum_$sTotalTournaments""")
            ),
          col(sGenre) === col("genre2"),
          "inner")
        .drop("genre2")

    lDataByGenre.show(false)

    // ajouter sum total par jeu sur twitch
  }
}
