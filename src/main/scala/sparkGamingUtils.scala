import org.apache.spark.sql.DataFrame
import sparkGaming.mSpark

import java.nio.file.{Files, Path, Paths}

object sparkGamingUtils {

  def readFromCsv(iPath: String): DataFrame = {
    if (Files.exists(Paths.get(iPath))) {
      mSpark
        .read
        .option("header", true)
        .csv(iPath)
    }else{
      mSpark.emptyDataFrame
    }

  }

  def saveCsvFromDataframe(iOutputPath:String, iDf: DataFrame): Unit = {
    if (Files.exists(Paths.get(iOutputPath))) {
      iDf
        .repartition(1)
        .write
        .format("csv")
        .mode("overwrite")
        .option("header", true)
        .save(iOutputPath)
    }
  }
}
