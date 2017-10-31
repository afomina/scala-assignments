package observatory

import com.sksamuel.scrimage.{Image, Pixel, RGBColor}

import scala.math._
import observatory.Visualization._
import observatory.Extraction._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction { //extends App {

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    Location(toDegrees(atan(sinh(Pi * (1.0 - 2.0 * tile.y.toDouble / (1 << tile.zoom))))),
      tile.x.toDouble / (1 << tile.zoom) * 360.0 - 180.0)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile):
  Image = {
    val pixels = (0 until 256 * 256)
      .par.map(pos => {
      val xPos: Int = (pos % 256) / 256 + tile.x // column of image as fraction with offset x
      val yPos: Int = (pos / 256) / 256 + tile.y // row of image as fraction with offset y

      val color = interpolateColor(
        colors,
        predictTemperature(
          temperatures,
          tileLocation(Tile(xPos, yPos, tile.zoom))
        )
      )

      pos -> RGBColor(color.red, color.green, color.blue, 127).toPixel
    })
      .seq
      .sortBy(_._1)
      .map(_._2).toArray

    Image(256, 256, pixels)
  }

//  override def main(args: Array[String]): Unit = {
//    val temps = locationYearlyAverageRecords(locateTemperatures(2010, "/stations.csv", "/2010.csv"))
//    val colors = List((60, Color(255, 255, 255)), (32, Color(255, 0, 0)), (12, Color(255, 255, 0)),
//      (0, Color(0, 255, 255)), (-15, Color(0, 0, 255)), (-27, Color(255, 0, 255)), (-50, Color(33, 0, 107)),
//      (-60, Color(0, 0, 0)))
//    tile(temps, colors)
//  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    for {
      z <- 0 to 3
      x <- 0 to 1 << z
      y <- 0 to 1 << z
      (year, data) <- yearlyData
    } generateImage(year, Tile(x, y, z), data)
  }

}
