package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math._

/**
 * 2nd milestone: basic visualization
 */
object Visualization extends App {

  val p = 2

  def distance(lat1: Double, long1: Double, lat2: Double, long2: Double): Double = {
    val angle = if (lat1 == lat2 && long1 == long2) 0
      else if (lat2 == -lat1 && long2 == 180 - long1 || lat1 == -lat2 && long1 == 180 - long2) Pi
      else acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(abs(long1 - long2)))
    6371 * angle
  }

  def distance(loc1: Location, loc2: Location): Double = distance(loc1.lat, loc1.lon, loc2.lat, loc2.lon)

  /**
   * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
   * @param location Location where to predict the temperature
   * @return The predicted temperature at `location`
   */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    def wi(x: Location) = 1 / pow(distance(x, location), p)

    val i = checkZero(temperatures.map(_._1), location)
    if (i == -1) {
      temperatures.map(t => wi(t._1) * t._2).sum / temperatures.map(t => wi(t._1)).sum
    } else temperatures.toList(i)._2
  }

  override def main(args: Array[String]): Unit = {
    val temps = Extraction.locationYearlyAverageRecords(Extraction.locateTemperatures(2010, "/stations.csv", "/2010.csv"))
    println(predictTemperature(temps, Location(-78, 106)))
  }

  def checkZero(locations: Iterable[Location], x: Location): Integer = {
    var list = locations
    var i = 0
    while (!list.isEmpty) {
      if (distance(x, list.head) == 0) return i
      i = i + 1
      list = list.tail
    }
    -1
  }

  /**
   * @param points Pairs containing a value and its associated color
   * @param value The value to interpolate
   * @return The color that corresponds to `value`, according to the color scale defined by `points`
   */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    ???
//    var sorted = points.toList.sortWith((a, b) => a._1 < b._1)
//    def binSearch(): (Temperature, Temperature) = {
//      while (sorted.head._1 < value) {
//        sorted = sorted.tail
//      }
//    }
  }

  /**
   * @param temperatures Known temperatures
   * @param colors Color scale
   * @return A 360×180 image where each pixel shows the predicted temperature at its location
   */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    ???
  }

}

