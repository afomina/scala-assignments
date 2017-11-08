package observatory

import java.nio.file.Paths
import java.time.LocalDate

import monix.types.utils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 1st milestone: data extraction
  */
object Extraction {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .config("spark.master", "local")
      .getOrCreate()

  import spark.implicits._
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String)
  : Iterable[(LocalDate, Location, Temperature)] = {
    val tempDf = readTemp(temperaturesFile)
    val stationsDf = readStations(stationsFile).filter(!col("Lat").isNull && !col("Long").isNull )

    val df = tempDf.join(stationsDf, tempDf("id") <=> stationsDf("id"), "left")

    def celsius(temp: Double): Double = (temp - 32) / 1.8

    df.collect().map {
      case Row(id: String, month: Int, day: Int, temp: Double, lat: Double, long: Double) =>
        (LocalDate.of(year, month, day), Location(lat, long), celsius(temp))
    }.toSeq
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    def aver(list: List[Double]) : Double = list.foldRight(0.0)(_ + _) / list.size

    records.map(t => (t._2, t._3)).groupBy(_._1)
      .map(z => (z._1, aver(z._2.map(_._2).toList))).toSeq

  }

  def readTemp(resource: String): DataFrame = {
    spark.read.csv(fsPath(resource))
      .select(('_c0 + "_" + coalesce('_c1, lit(""))).alias("id"), '_c2.alias("month").cast(IntegerType),
        '_c3.alias("day").cast(IntegerType), '_c4.alias("temp").cast(DoubleType)).where('_c4 < 9000 && ('_c0.isNotNull || '_c1.isNotNull))
  }

  def readStations(resource: String): DataFrame =
    spark.read.csv(fsPath(resource))
      .select(('_c0 + "_" + coalesce('_c1, lit(""))).alias("id"), '_c2.alias("lat").cast(DoubleType),
        '_c3.alias("long").cast(DoubleType))
      .where('_c2.isNotNull && '_c3.isNotNull && ('_c0.isNotNull || '_c1.isNotNull))

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def dfSchema(columnNames: List[String]): StructType =
    StructType(StructField(columnNames.head, StringType, nullable = true) :: StructField(columnNames.tail.head, StringType, nullable = true) ::
      columnNames.tail.tail.map(name => StructField(name, DoubleType, nullable = true)))

  def stationsSchema(columnNames: List[String]): StructType =
    StructType(StructField(columnNames.head, StringType, nullable = true) :: StructField(columnNames.tail.head, StringType, nullable = true) ::
      columnNames.tail.tail.map(name => StructField(name, DoubleType, nullable = false)))

  def row(line: List[String]): Row =
   Row(line.head :: line.tail.head :: line.tail.tail.map(_.toDouble).toList)

}
