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
    val fil = points.filter(_._1 == value)
    if (!fil.isEmpty)
      fil.toList(0)._2
    else {
      var sorted = points.toList.sortWith((a, b) => a._1 < b._1)
      val ab = binSearch()
      linearInterpolation(ab._1, ab._2)

      def binSearch(): ((Temperature, Color), (Temperature, Color)) = {
        var a: Temperature = -100
        while (sorted.head._1 < value) {
          a = sorted.head._1
          sorted = sorted.tail
        }
        (sorted.head, sorted.tail.head)
      }

      def linearInterpolation(a: (Temperature, Color), b: (Temperature, Color)): Color = {
        Color(intInterpolate(a._2.red, b._2.red),
          intInterpolate(a._2.green, b._2.green),
          intInterpolate(a._2.blue, b._2.blue))

        def intInterpolate(fx0: Integer, fx1: Integer): Int = (fx0 + (fx1 - fx0) * (value - a._1) / (b._1 - a._1)).toInt
      }
    }
  }

  /**
   * @param temperatures Known temperatures
   * @param colors Color scale
   * @return A 360×180 image where each pixel shows the predicted temperature at its location
   */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val colorsList = colors.toList
    val tempList = temperatures.toList

    var pixels = new Array[Pixel](360 * 180)
    var  i = 0
    for (lat <- (-89 to 90).reverse) {
      for (lon <- -180 until 180) {
        val tempAtLoc = tempList.filter(t => t._1.lat == lat && t._1.lon == lon)(0)._2
        val color = colorsList.filter(_._1 == tempAtLoc)(0)._2

        pixels(i) = Pixel.apply(color.red, color.green, color.blue, 0)
      }
    }

    Image(360, 180, pixels)
  }

}

