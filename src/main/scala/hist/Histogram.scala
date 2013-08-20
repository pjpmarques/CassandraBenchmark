
package hist

/**
 * Generate an histogram for a list of data.
 * The result will be an array containing the count for each bin that was specified. There will be an extra
 * lines which represents the samples that go from the last bin limit to +infinity.
 *
 * Example:
 *   val data = List(-1.0, 1.0, 2.0, 3.0, 4.0, 5.0)
 *   val bins = List(0.0, 3.0, 4.0)
 *
 *   val hist = new Histogram(data, bins)
 *   println(hist.result)                       // Will produce (1, 3, 1, 1)
 *   hist.print()                               // Will produce the lines shown next
 *
 * -----------------------------------------------------------------------------------
 *
 *      0.0:          1 ( 16.7%) | ****************
 *      3.0:          3 ( 50.0%) | **************************************************
 *      4.0:          1 ( 16.7%) | ****************
 * Infinity:          1 ( 16.7%) | ****************
 *    TOTAL:          6 (100.0%) | ****************************************************************************************************
 *
 * -----------------------------------------------------------------------------------
 *
 * @param data The data for which to generate the histogram.
 * @param bins The bins to use.
 */
class Histogram(val data: List[Double], val bins: List[Double]) {

  /** Contains the counts per bin plus one extra than represents the results from the last bin to +infinity */
  lazy val result: List[Int] = {
    val extendedBins = Double.NegativeInfinity :: bins ::: List(Double.PositiveInfinity)
    val result = new Array[Int](bins.length+1)

    for (bin <- 0 to bins.length) {
      result(bin) = data count { value => value > extendedBins(bin) && value <= extendedBins(bin+1) }
    }

    result.toList
  }

  /** Printout an histogram to the console */
  def print(): Unit = {
    val total = result.sum

    for (i <- 0 to bins.length) {
      val bars = "*" * (result(i)*100 / total).toInt
      val perc = 100.0 * result(i)/total
      println("%,10.1f: %,10d (%,5.1f%%) | %s".format(if (i<bins.length) bins(i) else Double.PositiveInfinity, result(i), perc, bars))
    }

    println("%10s: %,10d (%,5.1f%%) | %s".format("TOTAL", total, 100.0, "*" * 100))
  }
}
