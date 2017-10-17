package observatory

import java.nio.file.Paths
import java.time.LocalDate

import monix.types.utils
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
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
    val tempDf = readTemp(temperaturesFile, dfSchema(List("STN", "WBAN", "Month", "Day", "Temp")))
    val stationsDf = readStations(stationsFile).filter(!col("Lat").isNull && !col("Long").isNull && col("Lat") != 0.0 && col("Long")!=0.0)

    val df = tempDf.join(stationsDf, tempDf("STN") === stationsDf("STN")
      && tempDf("WBAN") === stationsDf("WBAN"))

    def celsius(temp: Double): Double = (temp - 32) / 1.8

    df.collect().map {
      case Row(stn: String, wban: String, month: Int, day: Int, temp: Double, lat: Double, long: Double) =>
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

      /*.aggregate((Location(0, 0), 0.0: Temperature))(
      (acc, tuple) => (tuple._1, acc._2 + tuple._2.map(_._2).sum / tuple._2.size),
      (acc1, acc2) => (acc1._1, acc1._2 + acc2._2)
    )*/

//    spark.sqlContext.createDataFrame(
//      spark.sparkContext.parallelize(records.toList)
//    ).groupBy("Location").agg(avg("Temperature")).map(row => (row.getAs(1), row.getAs(2))).collect()
  }

  def readTemp(resource: String, schema: StructType): DataFrame = {
    spark.read.csv(getClass.getResource(resource).getPath).select('_c0.alias("STN"), '_c1.alias("WBAN"), '_c2.alias("month"), '_c3.alias("day"), '_c4.alias("temp"))
   /* val rdd = spark.sparkContext.textFile(fsPath(resource))

    val data =
      rdd
        //.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

      spark.createDataFrame(data, schema)*/
  }

  def readStations(resource: String): DataFrame =
    spark.read.csv(resource).select('_c0.alias("STN"), '_c1.alias("WBAN"), '_c2.alias("lat"), '_c3.alias("long")).where(col("lat").isNotNull && col("long").isNotNull)

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

//  def tempRow(line: List[String]): Row =
//    Row(line)
}
