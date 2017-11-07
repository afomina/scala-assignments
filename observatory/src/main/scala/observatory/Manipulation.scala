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
//    g => map(g)
  }

  /**
   * @param temperatures Sequence of known temperatures over the years (each element of the collection
   *                     is a collection of pairs of location and temperature)
   * @return A function that, given a latitude and a longitude, returns the average temperature at this location
   */
  def average(temperatures: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val grids = temperatures.map(makeGrid)
    var pairs: List[(GridLocation, Temperature)] = List()

    for {
      lat <- -89 to 90
      lon <- -180 to 179
      g <- grids
    } {
      pairs = (GridLocation(lat, lon), g(GridLocation(lat, lon))) :: pairs
    }

    val aver = pairs.groupBy(_._1).map(a => (a._1, a._2.map(_._2).sum / a._2.size))

    aver
  }

  /**
   * @param temperatures Known temperatures
   * @param normals A grid containing the “normal” temperatures
   * @return A grid containing the deviations compared to the normal temperatures
   */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val grid = makeGrid(temperatures)
    val res = {
      for {
        lat <- -89 to 90
        lon <- -180 to 179
      }
        yield GridLocation(lat, lon) -> (grid(GridLocation(lat, lon)) - normals(GridLocation(lat, lon)))
    }.toMap
    res
  }

}

