package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math._

/**
 * 2nd milestone: basic visualization
 */
object Visualization {

  val p = 2

  def distance(lat1: Double, long1: Double, lat2: Double, long2: Double): Double = {
    val angle = if (lat1 == lat2 && long1 == long2) 0
      else if (lat2 == -lat1 && long2 == 180 - long1 || lat1 == -lat2 && long1 == 180 - long2) Pi
      else acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(abs(long1 - long2)))
    6371 * angle
  }

  /**
   * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
   * @param location Location where to predict the temperature
   * @return The predicted temperature at `location`
   */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    ???
  }

  /**
   * @param points Pairs containing a value and its associated color
   * @param value The value to interpolate
   * @return The color that corresponds to `value`, according to the color scale defined by `points`
   */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    ???
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

