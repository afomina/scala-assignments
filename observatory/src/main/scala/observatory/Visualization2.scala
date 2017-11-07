package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import scala.math._

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00 Top-left value
    * @param d01 Bottom-left value
    * @param d10 Top-right value
    * @param d11 Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
    point: CellPoint,
    d00: Temperature,
    d01: Temperature,
    d10: Temperature,
    d11: Temperature
  ): Temperature = {
    d00 * (1 - point.x) * (1 - point.y) + d10 * point.x * (1 - point.y) + d01 * (1 - point.x) * point.y + d11 * point.x * point.y
  }

  /**
    * @param grid Grid to visualize
    * @param colors Color scale to use
    * @param tile Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
    grid: GridLocation => Temperature,
    colors: Iterable[(Temperature, Color)],
    tile: Tile
  ): Image = {
    val pixels = new Array[Pixel](256 * 256)
    var  i = 0
    for (x <- 0 until 256) {
      for (y <- 0 until 256) {

        val location = Interaction.tileLocation(Tile(x / 256 + tile.x, y / 256 + tile.y, tile.zoom))
        val lat = location.lat
        val lon = location.lon

        val latRange = List(floor(lat).toInt, ceil(lat).toInt)
        val lonRange = List(floor(lon).toInt, ceil(lon).toInt)

        val d = {
          for {
            xPos <- 0 to 1
            yPos <- 0 to 1
          } yield (xPos, yPos) -> grid(GridLocation(latRange(1 - yPos), lonRange(xPos)))
        }.toMap

        val color = Visualization.interpolateColor(colors, bilinearInterpolation(CellPoint(latRange(1) - lat, lon - lonRange(0)),
  d((0, 0)), d((0, 1)), d((1, 0)), d((1, 1)) ))

        pixels(i) = Pixel.apply(color.red, color.green, color.blue, 100)
        i = i + 1
      }
    }

    Image(256, 256, pixels)
  }

}
