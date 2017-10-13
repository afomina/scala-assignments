package observatory

import java.time.LocalDate

import observatory.Extraction._

object Main extends App {

  def main() = {
    val iter: List[(LocalDate, Location, Temperature)] = locateTemperatures(1975, "/stations.csv", "/1975.csv").toList
    println(iter)
  }
}
