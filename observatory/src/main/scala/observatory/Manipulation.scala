package observatory

import scala.math._

/**
  * 4th milestone: value-added information
  */
object Manipulation {

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    {
      for {
      lat <- -89 to 90
      lon <- -180 to 179
    }
        yield GridLocation(lat, lon) -> Visualization.predictTemperature(temperatures, Location(lat, lon))
    }.toMap
  }

  /**
    * @param temperatures Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperatures: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val aver = temperatures.flatten.groupBy(_._1).map(a => (a._1, a._2.map(_._2).sum / a._2.size))
    val temp = {
      for {
        lat <- -89 to 90
        lon <- -180 to 179
      }
        yield GridLocation(lat, lon) -> Visualization.predictTemperature(aver, Location(lat, lon))
    }.toMap
    temp
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
//    val temperaturesMap = temperatures.map()
    {
      for {
        lat <- -89 to 90
        lon <- -180 to 179
      }
        yield GridLocation(lat, lon) -> abs(normals(GridLocation(lat, lon)) -
          temperatures.filter(_._1 == Location(lat.toInt, lon.toInt)).toList(0)._2)
    }.toMap
  }


}

