
package db_test

import com.datastax.driver.core._
import collection.JavaConversions._
import java.io.PrintWriter
import hist.Histogram
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.cli._

/**
 * Benchmarking class for Cassandra.
 *
 * @param address The cluster address (IP).
 * @param limit The number of lines to insert into the database.
 */
class CassandraTest(val address: String, val limit: Int = 10000) {
  val randomSeed = 595959

  //---------------------------------------------------------------------------------

  val cluster = Cluster.builder.addContactPoint(address).build        // Cluster identifier
  val session = cluster.connect()                                     // Session identifier

  //---------------------------------------------------------------------------------

  /** Shutdown the cluster */
  def shutdown(): Unit =
    cluster.shutdown()

  /** Printout information regarding the test and the cluster **/
  def info(): Unit = {
    println("  - Doing a simple test for %,d values" format limit)

    val metadata = cluster.getMetadata
    println("  - Connected to cluster '%s'." format metadata.getClusterName)
  }

  /** Create database (or drops it and recreates it from scratch */
  def createSchema(): Unit = {
    try {
      session.execute("DROP KEYSPACE cassandra_test;")
    }
    catch {
      case e : Exception => println("  - Keyspace 'cassandra_test' didn't exist, excellent! Carrying on. (%s)" format e.getMessage)
    }

    session.execute("CREATE KEYSPACE cassandra_test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")

    println("  - Created keyspace 'cassandra_test'.")

    session.execute("CREATE TABLE cassandra_test.mytable (" +
      "id varchar primary key," + Array.range(1,25).map("m%02d double" format _).mkString(",") + ");")

    println("  - Created table 'mytable'.")
  }

    /**
     * Load data into the cluster performing a "write test".
     *
     * The test consists in inserting a certain number of lines where each line consists in an identifier and 24 measures (doubles).
     * All times are recorded so that detailed statistics can be calculated (return of the function).
     *
     * @return Array with all the time samples it took to insert the data.
     */
  def writeTest(): Array[Long] = {
    val samples = new Array[Long](limit)

    val randomIds = new util.Random(randomSeed)
    val random = new util.Random
    val insertStm = session.prepare(
      "INSERT INTO cassandra_test.mytable(id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24)" +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

    println("  - Now inserting data...")

    // Main test phase, do "limit" inserts into the table

    val t1 = System.nanoTime

    for (i <- 1 to limit) {
      val id = randomIds.nextLong.toString
      val m = new Array[java.lang.Double](24) map { _ => new java.lang.Double(random.nextDouble) }

      val tA = System.nanoTime
      val bound = insertStm bind (id, m(0), m(1), m(2), m(3), m(4), m(5), m(6), m(7), m(8), m(9), m(10), m(11), m(12), m(13), m(14), m(15), m(16), m(17), m(18), m(19), m(20), m(21), m(22), m(23))
      session.execute(bound)
      val tB = System.nanoTime

      samples(i-1) = tB-tA

      if (i%(limit/20) == 0) {
        val elapsed = System.nanoTime - t1
        val throughput = 1.0e9 * i / elapsed
        println("  - Inserted %,7d elements in %,6.1f sec (%,.0f elem/sec)" format (i, elapsed/1.0e9, throughput))
      }
    }

    // Test is completed at this point

    val t2 = System.nanoTime
    val throughput = 1.0e9 * limit / (t2 - t1)

    val realTime = samples.sum
    val realThroughput = 1.0e9 * limit / realTime

    println("  - Done inserting data. Total Time = %,.0f sec. Total elements = %,d. Throughput = %,.0f elem/sec" format ((t2-t1)/1.0e9, limit, throughput))
    println("  - Done inserting data. Total Time = %,.0f sec. Total elements = %,d. Throughput = %,.0f elem/sec" format (realTime/1.0e9, limit, realThroughput))

    // Return the samples
    samples
  }

  /**
   * Performs a "read test".
   *
   * The test consists in reading a certain number of lines where each line consists in an identifier and 24 measures (doubles).
   * All times are recorded so that detailed statistics can be calculated (return of the function).
   *
   * @return Array with all the time samples it took to insert the data.
   */
  def readTest(): Array[Long] = {
    val samples = new Array[Long](limit)
    var totalRead = 0

    val randomIds = new util.Random(randomSeed)
    val selectStm = session.prepare("SELECT * FROM cassandra_test.mytable WHERE id = ?;")

    println("  - Now reading data...")

    // Main test phase, do "limit" reads from the table

    val t1 = System.nanoTime

    for (i <- 1 to limit) {
      val id = randomIds.nextLong.toString

      val tA = System.nanoTime
      val bound = selectStm bind id
      val result = session.execute(bound)
      totalRead+= result.all.length
      val tB = System.nanoTime

      samples(i-1) = tB-tA

      if (i%(limit/20) == 0) {
        val elapsed = System.nanoTime - t1
        val throughput = 1.0e9 * i / elapsed
        println("  - Read %,7d elements out of %,7d tried in %,6.1f sec (%,.0f elem/sec)" format (totalRead, i, elapsed/1.0e9, throughput))
      }
    }

    // Test is completed at this point

    val t2 = System.nanoTime
    val throughput = 1.0e9 * limit / (t2 - t1)

    val realTime = samples.sum
    val realThroughput = 1.0e9 * limit / realTime

    println("  - Done reading data. Total Time = %,.0f sec. Total elements = %,d out of %,d tried. Throughput = %,.0f elem/sec" format ((t2-t1)/1.0e9, totalRead, limit, throughput))
    println("  - Done reading data. Total Time = %,.0f sec. Total elements = %,d out of %,d tried. Throughput = %,.0f elem/sec" format (realTime/1.0e9, totalRead, limit, realThroughput))

    // Return the samples
    samples
  }

  /**
   * Saves the results to file and displays an histogram of the results.
   *
   * @param samples An array containing all the times (in nanosecs) for the performed operations
   * @param fileSufix Suffix to use in the output file
   */
  def dumpStats(samples: Array[Long], fileSufix: String): Unit = {
    // Write all data into file

    val fileName = new SimpleDateFormat("yyyy-mm-dd_hh:mm:ss").format(new Date) + ("__%s.csv" format fileSufix)
    println("  - Writing the results into '%s' (%,d lines)" format (fileName, limit))

    val pw = new PrintWriter(fileName)
    samples.foreach { pw.println }
    pw.close()

    // Show an histogram of the results

    val bins = List[Double](0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 10.0, 20.0)
    val histogram = new Histogram(samples.map { _/100000 / 10.0 }.toList, bins)

    println("  - Here's a histogram of the latencies (in msec):")
    histogram.print()
  }

  /** Query the database for some ids */
  def simpleQuery(): Unit = {
    println("  - Getting a few ids: ")

    val result = session.execute("SELECT id FROM cassandra_test.mytable LIMIT 3;")
    for (elem <- result) {
      println("\t %s" format elem.getString(0))
    }
  }
}

/** Main test driver */
object CassandraTest  {
  val defaultTestSize = 10000
  var limit = defaultTestSize
  var writeTest = true
  var readTest = true
  var ip = "127.0.0.1"

  def parseCommandLine(args: Array[String]) = {
    val prgOptions = new Options
    prgOptions.addOption("help", false, "help, prints this message")
    prgOptions.addOption("read", false, "read test")
    prgOptions.addOption("write", false, "write test")
    prgOptions.addOption("size", true, "size of the test (default=%,d)" format defaultTestSize)
    prgOptions.addOption("ip", true, "ip address where cassandra is (default=127.0.0.1)")

    val cmdOptions = (new GnuParser).parse(prgOptions, args)

    if (cmdOptions.getArgs.length != 0) {
      System.err.println("Invalid option specified.")
      (new HelpFormatter).printHelp("CassandraTest", prgOptions, true)
      System.exit(0)
    }

    if (cmdOptions hasOption "help") {
      (new HelpFormatter).printHelp("CassandraTest", prgOptions, true)
      System.exit(0)
    }

    if (cmdOptions hasOption "size") {
      limit = (cmdOptions getOptionValue "size").toInt
    }

    if (cmdOptions hasOption "ip") {
      ip = cmdOptions getOptionValue "ip"
    }

    readTest = cmdOptions hasOption "read"
    writeTest = cmdOptions hasOption "write"
  }

  def main(args: Array[String]) {
    parseCommandLine(args)

    val test = new CassandraTest(ip, limit)
    test.info()

    if (writeTest) {
      test.createSchema()
      val samples = test.writeTest()
      test.simpleQuery()
      test.dumpStats(samples, "write_test")
    }

    if (readTest) {
      val samples = test.readTest()
      test.dumpStats(samples, "read_test")
    }

    test.shutdown()
  }
}
