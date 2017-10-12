package observatory

import java.nio.file.Paths
import java.time.LocalDate

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

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String)
  : Iterable[(LocalDate, Location, Temperature)] = {
    val tempDf = read(temperaturesFile, List("STN", "WBAN", "Month", "Day", "Temp"))
    val stationsDf = read(stationsFile, List("STN", "WBAN", "Lat", "Long")).filter(row => row.getDouble(2) != null && row.getDouble(3) != null)

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
      .map(z => (z._1, aver(z._2.map(_._2).toList)))

      /*.aggregate((Location(0, 0), 0.0: Temperature))(
      (acc, tuple) => (tuple._1, acc._2 + tuple._2.map(_._2).sum / tuple._2.size),
      (acc1, acc2) => (acc1._1, acc1._2 + acc2._2)
    )*/

//    spark.sqlContext.createDataFrame(
//      spark.sparkContext.parallelize(records.toList)
//    ).groupBy("Location").agg(avg("Temperature")).map(row => (row.getAs(1), row.getAs(2))).collect()
  }

  def read(resource: String, headerColumns: List[String]): DataFrame = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        //.mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

      spark.createDataFrame(data, schema)
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def dfSchema(columnNames: List[String]): StructType =
    StructType(StructField(columnNames.head, StringType, nullable = true) :: StructField(columnNames.tail.head, StringType, nullable = true) ::
      columnNames.tail.tail.map(name => StructField(name, DoubleType, nullable = true)))

  def row(line: List[String]): Row =
   Row(line.head.toString :: line.tail.head.toString :: line.tail.tail.map(_.toDouble): _*)
}
