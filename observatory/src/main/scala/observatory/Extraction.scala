package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.io.Source

/**
  * 1st milestone: data extraction
  */
object Extraction {

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String)
  : Iterable[(LocalDate, Location, Temperature)] = {
    val tempDf = readTemp(temperaturesFile, year)
    val stationsDf = readStations(stationsFile)

    def celsius(temp: Double): Double = (temp - 32) / 1.8

    tempDf.map{
      case TempRecord(id, date, temp) => (date, stationsDf.find(_.id == id).get.location, celsius(temp))
    }

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

  case class TempRecord (id: (String, String), date: LocalDate, temp: Double)

  case class StationRecord(id: (String, String), location: Location)

  def readTemp(resource: String, year: Int): List[TempRecord] = {
    Source.fromInputStream(getClass.getResourceAsStream(resource)).getLines().map{
      line => val tuple = line.split(",")
        TempRecord((tuple(0), tuple(1)), LocalDate.of(year, tuple(2).toInt, tuple(3).toInt), tuple(4).toDouble)
    }.filter(_.temp < 9000).toList
  }

  def readStations(resource: String): List[StationRecord] = {
    Source.fromInputStream(getClass.getResourceAsStream(resource)).getLines().map(_.split(","))
      .filter(t => t.size > 2 && t(2) != null && t(3) != null).map(
       tuple => StationRecord((tuple(0), tuple(1)), Location(tuple(2).toDouble, tuple(3).toDouble))).toList
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

}
