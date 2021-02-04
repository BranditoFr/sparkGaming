
import org.apache.spark.sql.DataFrame
import sparkGaming.spark

object sparkGamingUtils {

  def readFromCsv(iPath: String): DataFrame = {
    spark
      .read
      .option("header", true)
      .csv(iPath)
  }

  def saveCsvFromDataframe(iOutputPath:String, iDf: DataFrame): Unit = {
    iDf
      .write
      .format("csv")
      .mode("overwrite")
      .option("header",true)
      .save(iOutputPath)
  }
}
